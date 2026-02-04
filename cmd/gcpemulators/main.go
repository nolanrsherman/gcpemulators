// This package is the main entry point for the gcpemulators command.
// It is used to start the emulators for the various GCP services.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nolanrsherman/gcpemulators/cmd/gcpemulators/cli"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("received shutdown signal, shutting down...")
		cancel()
	}()

	if err := cli.Execute(ctx, logger); err != nil {
		logger.Error("command failed", zap.Error(err))
		os.Exit(1)
	}
}
