// mockprovider is the standalone HTTP/1.1 test upstream (12.1).
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"gaigate/internal/mock"
)

func main() {
	logger := mock.NewLogger(os.Stdout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Getenv("MOCK_ADDR"), logger)
	stop()
	if err != nil {
		logger.Error("mockprovider failed", "err", err.Error())
		os.Exit(1)
	}
}
