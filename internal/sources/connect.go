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

// ConnectTimeout bounds a deferred connection. The attempt is shared by every
// caller waiting on it, so it cannot take any single caller's deadline; this
// ceiling is what stops a partitioned network from pinning it open. It is
// sized for a cold cloud connector path (token fetch, instance metadata
// lookup, TLS handshake) rather than for a healthy connection.
const ConnectTimeout = 60 * time.Second

// ConnectOnce holds a connection a source builds on first use. A source that
// supports deferred initialization keeps one of these instead of a bare
// handle, and its accessors resolve through Do.
//
// Construct it during Initialize so it captures the startup context: the
// connect itself runs from whichever request triggers it, and a request
// context carries neither the tracer nor the same user agent.
type ConnectOnce[T any] struct {
	name       string
	sourceType string
	tracer     trace.Tracer
	userAgent  string

	mu    sync.RWMutex
	value T
	ready bool

	// initGroup, not mu, is what serializes connecting: mu is held only across
	// field access, never across the connect. A mutex held for the length of a
	// connect would also block a caller from abandoning a hung attempt, which
	// the select in Do relies on being able to do.
	initGroup singleflight.Group
}

// NewConnectOnce returns a holder for a connection that has not been made yet.
// ctx must be the context Initialize was called with, so the user agent it
// carries — which includes --user-agent-metadata — is the one every later
// connect reports.
func NewConnectOnce[T any](ctx context.Context, name, sourceType string, tracer trace.Tracer) *ConnectOnce[T] {
	userAgent, _ := util.UserAgentFromContext(ctx)
	return &ConnectOnce[T]{name: name, sourceType: sourceType, tracer: tracer, userAgent: userAgent}
}

// Get returns the connection if one has already been made. It never blocks and
// never fails, so paths that must not pay for a connect can ask without
// triggering one.
func (c *ConnectOnce[T]) Get() (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value, c.ready
}

// Do returns the connection, making it on the first call. Concurrent callers
// share one attempt; a failed attempt is not remembered, so a source that was
// down starts working on a later call without a restart.
func (c *ConnectOnce[T]) Do(ctx context.Context, connect func(context.Context) (T, error)) (T, error) {
	var zero T
	if value, ok := c.Get(); ok {
		return value, nil
	}

	ch := c.initGroup.DoChan("", func() (any, error) {
		// A caller that queued behind a winner which already finished would
		// otherwise start a second connect, since singleflight only shares an
		// attempt that is still in flight.
		if value, ok := c.Get(); ok {
			return value, nil
		}

		// The attempt is shared by every caller waiting on it, so it must not
		// inherit the cancellation of whichever caller happened to start it.
		// WithoutCancel keeps the trace context so the span still parents.
		connectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ConnectTimeout)
		defer cancel()

		// Drivers read the user agent from the context they connect with.
		// Eager init runs under the startup context, which carries
		// --user-agent-metadata; a deferred connect runs under a request
		// context, which does not.
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
		// Only this caller gives up; the shared attempt runs on for the others
		// and caches the connection if it succeeds.
		return zero, fmt.Errorf("unable to initialize source %q: %w", c.name, ctx.Err())
	}
}
