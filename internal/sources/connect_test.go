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

package sources_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/googleapis/mcp-toolbox/internal/sources"
	"github.com/googleapis/mcp-toolbox/internal/testutils"
	"github.com/googleapis/mcp-toolbox/internal/util"
	"go.opentelemetry.io/otel/trace/noop"
)

// handle stands in for whatever a source connects to.
type handle struct{ id int }

// connector records how a source's connect function was called, so tests can
// observe coalescing, retries and the context the connect actually saw.
type connector struct {
	mu     sync.Mutex
	calls  int
	err    error
	delay  time.Duration
	ctxErr error
	userAg string
}

func (c *connector) connect(ctx context.Context) (*handle, error) {
	c.mu.Lock()
	c.calls++
	id, err, delay := c.calls, c.err, c.delay
	c.mu.Unlock()

	ua, _ := util.UserAgentFromContext(ctx)
	c.mu.Lock()
	c.userAg = ua
	c.mu.Unlock()

	// Holding the connection open lets concurrent callers pile up behind it, so
	// a missing singleflight shows up as extra calls. Watching ctx here is what
	// lets a test observe whether the shared attempt inherited a cancellation.
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		c.mu.Lock()
		c.ctxErr = ctx.Err()
		c.mu.Unlock()
		return nil, ctx.Err()
	}

	if err != nil {
		return nil, err
	}
	return &handle{id: id}, nil
}

func (c *connector) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *connector) connectContextErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctxErr
}

func (c *connector) observedUserAgent() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.userAg
}

func newConnectOnce(ctx context.Context) *sources.ConnectOnce[*handle] {
	return sources.NewConnectOnce[*handle](ctx, "my-source", "mock", noop.NewTracerProvider().Tracer("test"))
}

func TestConnectOnceCoalescesConcurrentCallers(t *testing.T) {
	c := &connector{delay: 50 * time.Millisecond}
	once := newConnectOnce(context.Background())

	const callers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = once.Do(context.Background(), c.connect)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d failed to connect: %s", i, err)
		}
	}
	if got := c.callCount(); got != 1 {
		t.Fatalf("expected the racing callers to share one connect, got %d", got)
	}
}

func TestConnectOnceSurvivesFirstCallerCancellation(t *testing.T) {
	c := &connector{delay: 100 * time.Millisecond}
	once := newConnectOnce(context.Background())

	// The first caller starts the shared attempt and then walks away. Its
	// context must not travel into the connect, or every other caller waiting
	// on the same attempt fails for a request that is no longer theirs.
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstStarted := make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		close(firstStarted)
		_, err := once.Do(firstCtx, c.connect)
		firstErr <- err
	}()
	<-firstStarted

	secondErr := make(chan error, 1)
	go func() {
		// Give the first caller time to win the singleflight.
		time.Sleep(20 * time.Millisecond)
		cancelFirst()
		_, err := once.Do(context.Background(), c.connect)
		secondErr <- err
	}()

	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the cancelled caller to see its own cancellation, got %v", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("a cancelled caller must not fail the others waiting: %s", err)
	}
	if err := c.connectContextErr(); err != nil {
		t.Fatalf("the connect saw a cancelled context: %s", err)
	}
	if _, ok := once.Get(); !ok {
		t.Fatal("expected the shared attempt to finish and cache the connection")
	}
}

func TestConnectOnceRespectsCallerDeadline(t *testing.T) {
	c := &connector{delay: time.Minute}
	once := newConnectOnce(context.Background())

	// A hung connect must not pin the request for ConnectTimeout: the caller
	// returns on its own deadline and the agent gets a readable error.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := once.Do(ctx, c.connect)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller's deadline to end the wait, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("caller waited %s, well past its own deadline", elapsed)
	}
}

func TestConnectOnceRetriesAfterFailure(t *testing.T) {
	c := &connector{err: errors.New("connection refused")}
	once := newConnectOnce(context.Background())

	if _, err := once.Do(context.Background(), c.connect); err == nil {
		t.Fatal("expected the first connect to fail")
	}
	if _, ok := once.Get(); ok {
		t.Fatal("a failed connection must not be cached")
	}

	// A failure is not cached, so a source that comes up later starts working
	// without restarting the server.
	c.mu.Lock()
	c.err = nil
	c.mu.Unlock()

	if _, err := once.Do(context.Background(), c.connect); err != nil {
		t.Fatalf("expected the retry to succeed, got %s", err)
	}
	if got := c.callCount(); got != 2 {
		t.Fatalf("expected 2 connect attempts, got %d", got)
	}
}

func TestConnectOnceReusesTheConnection(t *testing.T) {
	c := &connector{}
	once := newConnectOnce(context.Background())

	first, err := once.Do(context.Background(), c.connect)
	if err != nil {
		t.Fatalf("unexpected error connecting: %s", err)
	}
	second, err := once.Do(context.Background(), c.connect)
	if err != nil {
		t.Fatalf("unexpected error on the second call: %s", err)
	}
	if first != second {
		t.Fatalf("expected the same connection, got %v and %v", first, second)
	}
	if got := c.callCount(); got != 1 {
		t.Fatalf("expected the connection to be reused, got %d attempts", got)
	}
}

func TestConnectOnceRestoresStartupUserAgent(t *testing.T) {
	// Drivers read the user agent from the context they connect with. A
	// deferred connect runs from a request context, whose user agent omits
	// --user-agent-metadata, so the startup value has to be reapplied or a
	// lazily connected source would identify itself differently than an
	// eagerly connected one.
	startupCtx := testutils.ContextWithUserAgent(context.Background(), "1.2.3+custom-metadata")
	once := newConnectOnce(startupCtx)

	c := &connector{}
	callerCtx := testutils.ContextWithUserAgent(context.Background(), "1.2.3")
	if _, err := once.Do(callerCtx, c.connect); err != nil {
		t.Fatalf("unexpected error connecting: %s", err)
	}

	want := "genai-toolbox/1.2.3+custom-metadata"
	if got := c.observedUserAgent(); got != want {
		t.Errorf("connect saw user agent %q, want %q", got, want)
	}
}

func TestConnectOnceKeepsCallerUserAgentWhenStartupHadNone(t *testing.T) {
	once := newConnectOnce(context.Background())

	c := &connector{}
	callerCtx := testutils.ContextWithUserAgent(context.Background(), "1.2.3")
	if _, err := once.Do(callerCtx, c.connect); err != nil {
		t.Fatalf("unexpected error connecting: %s", err)
	}

	want := "genai-toolbox/1.2.3"
	if got := c.observedUserAgent(); got != want {
		t.Errorf("connect saw user agent %q, want %q", got, want)
	}
}
