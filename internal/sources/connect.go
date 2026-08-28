// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sources

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/googleapis/mcp-toolbox/internal/util"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"
)

// ConnectTimeout is the default ceiling on a connection attempt. It is sized
// for a cold cloud connector path rather than for a healthy connection.
const ConnectTimeout = 60 * time.Second

// Option configures a ConnectOnce.
type Option func(*options)

type options struct {
	timeout time.Duration
}

// WithConnectTimeout raises the ceiling for a source whose own configuration
// allows a longer connect. A shorter value is ignored.
func WithConnectTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = d }
}

// ConnectOnce holds a connection a source builds on first use. A source keeps
// one of these instead of a bare handle and resolves it through Do.
type ConnectOnce[T any] struct {
	name       string
	sourceType string
	tracer     trace.Tracer
	userAgent  string
	timeout    time.Duration

	mu    sync.RWMutex
	value T
	ready bool

	// initGroup, not mu, serializes connecting. A mutex held for the length of
	// a connect would block a caller from abandoning a hung attempt.
	initGroup singleflight.Group
}

// NewConnectOnce returns a holder for a connection that has not been made yet.
// ctx must be the context Initialize was called with, so that the user agent it
// carries is the one every later connect reports.
func NewConnectOnce[T any](ctx context.Context, name, sourceType string, tracer trace.Tracer, opts ...Option) *ConnectOnce[T] {
	userAgent, _ := util.UserAgentFromContext(ctx)
	o := options{timeout: ConnectTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	if o.timeout < ConnectTimeout {
		o.timeout = ConnectTimeout
	}
	return &ConnectOnce[T]{name: name, sourceType: sourceType, tracer: tracer, userAgent: userAgent, timeout: o.timeout}
}

// Get returns the connection if one has already been made. It never blocks and
// never fails.
func (c *ConnectOnce[T]) Get() (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value, c.ready
}

// Do returns the connection, making it on the first call. Concurrent callers
// share one attempt, and a failed attempt is not remembered.
func (c *ConnectOnce[T]) Do(ctx context.Context, connect func(context.Context) (T, error)) (T, error) {
	var zero T
	if value, ok := c.Get(); ok {
		return value, nil
	}

	ch := c.initGroup.DoChan("", func() (any, error) {
		// singleflight only shares an attempt that is still in flight, so a
		// caller queued behind a finished winner would start a second connect.
		if value, ok := c.Get(); ok {
			return value, nil
		}

		// The attempt is shared by every waiter, so it must not inherit the
		// cancellation of whichever caller started it.
		connectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
		defer cancel()

		// A deferred connect runs under a request context, whose user agent
		// omits --user-agent-metadata.
		if c.userAgent != "" {
			connectCtx = util.WithUserAgentValue(connectCtx, c.userAgent)
		}

		childCtx, span := c.tracer.Start(
			connectCtx,
			"toolbox/server/source/init",
			trace.WithAttributes(attribute.String("source_type", c.sourceType)),
			trace.WithAttributes(attribute.String("source_name", c.name)),
		)
		defer span.End()

		value, err := connect(childCtx)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("unable to initialize source %q: %w", c.name, err)
		}

		c.mu.Lock()
		c.value, c.ready = value, true
		c.mu.Unlock()
		return value, nil
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return zero, res.Err
		}
		value, _ := res.Val.(T)
		return value, nil
	case <-ctx.Done():
		// Only this caller gives up; the shared attempt runs on for the others.
		return zero, fmt.Errorf("unable to initialize source %q: %w", c.name, ctx.Err())
	}
}
