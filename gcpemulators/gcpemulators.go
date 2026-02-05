package gcpemulators

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/nolanrsherman/gcpemulators/gcpemulators/gcpemulatorspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Starts a cloud tasks emulator using docker and returns a cleanup
// function.

type newCloudTaskEmulatorOptions struct {
	port             int
	mongoUri         string
	dbName           string
	gcpemulatorsPath string
	command          string
}

func WithPort(port int) func(*newCloudTaskEmulatorOptions) {
	return func(o *newCloudTaskEmulatorOptions) {
		o.port = port
	}
}

type CloudTaskEmulator struct {
	Client gcpemulatorspb.GcpEmulatorClient
	Port   int
}

func findOpenPort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func NewCloudTaskEmulator(opts ...func(*newCloudTaskEmulatorOptions)) (cloudTasksEmulator *CloudTaskEmulator, cleanup func() error, err error) {
	options := &newCloudTaskEmulatorOptions{
		port:             findOpenPort(),
		mongoUri:         "mongodb://localhost:27017/?directConnection=true",
		dbName:           "cloudtaskemulator",
		gcpemulatorsPath: "gcpemulators",
		command:          "cloudtasks",
	}
	for _, opt := range opts {
		opt(options)
	}

	cmd := exec.Command(options.gcpemulatorsPath,
		options.command,
		"-p", strconv.Itoa(options.port),
		"--mongo-uri", options.mongoUri,
		"--db-name", options.dbName,
	)
	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}

	closeProcess := func() error {
		var err error

		pWait := make(chan error, 1)
		go func() {
			cmd.Process.Signal(os.Interrupt)
			_, err := cmd.Process.Wait()
			pWait <- err
		}()
		select {
		case processWaitError := <-pWait:
			if processWaitError != nil {
				err = errors.Join(err, processWaitError, cmd.Process.Kill())
			}
		case <-time.After(5 * time.Second):
			err = errors.Join(err, cmd.Process.Kill())
		}

		return err
	}

	grpcConn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", options.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	cloudTasksClient := gcpemulatorspb.NewGcpEmulatorClient(grpcConn)
	closeConnections := func() error {
		return grpcConn.Close()
	}

	ctx := context.Background()
	// Wait until the client is ready, polling every 100ms with a 10s timeout.
	ctxWaitTimeout, ctxWaitTimeoutCancel := context.WithTimeout(ctx, 5*time.Second)
	defer ctxWaitTimeoutCancel()

	// Start with shorter polling interval for faster startup detection
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

readyloop:
	for {
		select {
		case <-ctxWaitTimeout.Done():
			timeoutErr := fmt.Errorf("timeout waiting for emulator readiness: %w", ctxWaitTimeout.Err())
			closeErr := errors.Join(closeProcess(), closeConnections())
			return nil, nil, errors.Join(timeoutErr, closeErr)
		case <-ticker.C:
			readiness, err := cloudTasksClient.Readiness(ctxWaitTimeout, &gcpemulatorspb.ReadinessRequest{})
			if err == nil && readiness.Ready {
				break readyloop
			}
		}
	}

	return &CloudTaskEmulator{
			Client: cloudTasksClient,
			Port:   options.port,
		}, func() error {
			return errors.Join(closeProcess(), closeConnections())
		}, nil
}
