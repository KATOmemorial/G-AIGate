package main

import (
	"net/http"
	"sync"
)

// ownedHTTPServer tracks whole handlers, including their deferred cleanup and
// synchronous health reports. net/http.Close cancels connections but does not
// join these owners. Keep the wrapper outside Gin without wrapping the writer
// or recovering panics, so its protocol and ErrAbortHandler behavior survive.
type ownedHTTPServer struct {
	*http.Server
	handlers *httpHandlerOwners
}

type httpHandlerOwners struct {
	next     http.Handler
	mu       sync.Mutex
	draining bool
	active   sync.WaitGroup
}

func newHTTPServer(addr string, handler http.Handler) *ownedHTTPServer {
	if handler == nil {
		handler = http.DefaultServeMux
	}
	owners := &httpHandlerOwners{next: handler}
	return &ownedHTTPServer{Server: &http.Server{Addr: addr, Handler: owners}, handlers: owners}
}

func (h *httpHandlerOwners) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		// A connection accepted before Close may reach ServeHTTP late. It must
		// not enter application code after the shutdown join has begun.
		panic(http.ErrAbortHandler)
	}
	h.active.Add(1)
	h.mu.Unlock()
	defer h.active.Done()
	h.next.ServeHTTP(w, r)
}

func (h *httpHandlerOwners) wait() {
	h.mu.Lock()
	h.draining = true // serialize the final Add with Wait, including zero owners
	h.mu.Unlock()
	// Request operations and proxy/report cleanup are bounded and canceled by
	// Server.Close. Even an expired dependency deadline cannot permit closing
	// resources that a handler is still using; do not detach or renew a budget.
	h.active.Wait()
}
