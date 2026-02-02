package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

func main() {

	// capture sigint and sigterm and cancel the context
	ctx, cancel := context.WithCancel(context.Background())
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-interruptChan
		cancel()
	}()
	defer cancel()

	port := flag.Int("port", 6699, "The port to listen on")
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	emulator := NewCloudStorageEmulator(*port, logger)
	err = emulator.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
