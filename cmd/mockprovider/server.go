package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode"

	"gaigate/internal/mock"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"
)

const (
	defaultAddr    = "127.0.0.1:18080"
	shutdownBudget = 3 * time.Second
)

func mockAddr(raw string) (string, error) {
	if raw == "" {
		return defaultAddr, nil
	}
	for _, r := range raw {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", errors.New("MOCK_ADDR must not contain whitespace or control characters")
		}
	}
	_, port, err := net.SplitHostPort(raw)
	if err != nil {
		return "", fmt.Errorf("MOCK_ADDR must be host:port: %w", err)
	}
	for _, r := range port {
		if r < '0' || r > '9' {
			return "", errors.New("MOCK_ADDR port must be numeric")
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", errors.New("MOCK_ADDR port must be in 1..65535")
	}
	return raw, nil
}

func newHandler(e mock.Engine, logger *slog.Logger) http.Handler {
	r := gin.New()
	r.Use(mock.RequestLog(logger)) // No recovery middleware: abort reaches net/http.
	mock.RegisterChatRoutes(r, e)
	mock.RegisterControlRoutes(r, e)
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	return r
}

func run(ctx context.Context, rawAddr string, logger *slog.Logger) error {
	addr, err := mockAddr(rawAddr)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on MOCK_ADDR: %w", err)
	}
	gin.SetMode(gin.ReleaseMode)
	// Exactly one Engine shared by the chat and control routes.
	return serve(ctx, listener, newHandler(mock.NewEngine(), logger), logger)
}

func serve(ctx context.Context, listener net.Listener, handler http.Handler, logger *slog.Logger) error {
	// Signal cancellation initiates graceful shutdown; it must not immediately
	// cancel handlers that still have the 3s budget in which to complete.
	requestCtx, cancelRequests := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRequests()
	tracked := &handlerTracker{next: handler}
	srv := &http.Server{
		Handler:     tracked,
		BaseContext: func(net.Listener) context.Context { return requestCtx },
	}
	var group errgroup.Group
	done := make(chan struct{})
	group.Go(func() error {
		defer close(done) // The serving goroutine is the sole close owner.
		err := srv.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve mock HTTP: %w", err)
	})
	logger.Info("mockprovider_started", "addr", listener.Addr().String())
	select {
	case <-done:
		cancelRequests()
		closeErr := srv.Close()
		tracked.stopAndWait()
		return errors.Join(group.Wait(), closeErr)
	case <-ctx.Done():
		logger.Info("mockprovider_stopping")
	}
	err := shutdown(ctx, srv, tracked, cancelRequests, logger)
	serveErr := group.Wait()
	logger.Info("mockprovider_stopped")
	return errors.Join(err, serveErr)
}

// Shutdown waits gracefully for up to 3s. Close cancels blocked reads/writes on
// timeout; the separate request cancellation also releases all mock waits. The
// gate prevents any later Add racing with Wait, and Wait confirms handler exit.
func shutdown(ctx context.Context, srv *http.Server, tracked *handlerTracker, cancelRequests context.CancelFunc, logger *slog.Logger) error {
	budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownBudget)
	defer cancel()
	err := srv.Shutdown(budget)
	if err != nil {
		logger.Warn("mockprovider_shutdown_forced", "err", err.Error())
		err = srv.Close()
	}
	cancelRequests()
	tracked.stopAndWait()
	return err
}

type handlerTracker struct {
	next     http.Handler
	mu       sync.Mutex
	stopping bool
	active   sync.WaitGroup
}

func (h *handlerTracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	h.active.Add(1)
	h.mu.Unlock()
	defer h.active.Done()
	h.next.ServeHTTP(w, r)
}

func (h *handlerTracker) stopAndWait() {
	h.mu.Lock()
	h.stopping = true
	h.mu.Unlock()
	h.active.Wait()
}
