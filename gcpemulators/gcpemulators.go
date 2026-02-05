package gcpemulators

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/cloudtasksemulatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Starts a cloud tasks emulator using docker and returns a cleanup
// function.

type newCloudTaskEmulatorOptions struct {
	port     int
	imageTag string
	doPull   bool
}

func WithPort(port int) func(*newCloudTaskEmulatorOptions) {
	return func(o *newCloudTaskEmulatorOptions) {
		o.port = port
	}
}

func NewCloudTaskEmulator(opts ...func(*newCloudTaskEmulatorOptions)) (cloudTasksClient cloudtasksemulatorpb.CloudTasksEmulatorServiceClient, cleanup func() error, err error) {
	imageName := "nolanrs/gcpemulators"
	o := &newCloudTaskEmulatorOptions{
		port:     6441,
		imageTag: "v0.1.2",
	}
	for _, opt := range opts {
		opt(o)
	}

	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, nil, err
	}
	defer cli.Close()

	imageAndTag := fmt.Sprintf("%s:%s", imageName, o.imageTag)

	pullBuffer := bytes.NewBuffer([]byte{})
	if o.doPull {
		reader, err := cli.ImagePull(ctx, fmt.Sprintf("%s:%s", imageName, o.imageTag), image.PullOptions{})
		if err != nil {
			return nil, nil, err
		}

		defer reader.Close()
		// cli.ImagePull is asynchronous.
		// The reader needs to be read completely for the pull operation to complete.
		// If stdout is not required, consider using io.Discard instead of os.Stdout.
		io.Copy(pullBuffer, reader)
	}

	cloudTasksPort, err := nat.NewPort("tcp", strconv.Itoa(o.port))
	if err != nil {
		return nil, nil, err
	}

	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: imageAndTag,
		Cmd:   []string{"cloudtasks", "-p", strconv.Itoa(o.port)},
		Tty:   false,
		ExposedPorts: nat.PortSet{
			cloudTasksPort: struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			cloudTasksPort: []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: strconv.Itoa(o.port),
				},
			},
		},
	}, nil, nil, "")
	if err != nil {
		return nil, nil, err
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, nil, err
	}

	grpcConn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", o.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	cloudTasksClient = cloudtasksemulatorpb.NewCloudTasksEmulatorServiceClient(grpcConn)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		statusCh, errCh := cli.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
		select {
		case err := <-errCh:
			if err != nil {
				panic(err)
			}
		case <-statusCh:
		}
	}()

	// Wait until the client is ready, polling every 100ms with a 10s timeout.
	ctxWaitTimeout, ctxWaitTimeoutCancel := context.WithTimeout(ctx, 20*time.Second)
	defer ctxWaitTimeoutCancel()

	// Start with shorter polling interval for faster startup detection
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

readyloop:
	for {
		select {
		case <-ctxWaitTimeout.Done():
			_ = cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{
				Force: true,
			})
			return nil, nil, fmt.Errorf("timeout waiting for emulator readiness: %w", ctxWaitTimeout.Err())
		case <-ticker.C:
			readiness, err := cloudTasksClient.Readiness(ctxWaitTimeout, &cloudtasksemulatorpb.ReadinessRequest{})
			if err == nil && readiness.Ready {
				break readyloop
			}
		}
	}

	return cloudTasksClient, func() error {
		defer wg.Wait()
		defer grpcConn.Close()
		defer cli.Close()

		// capture container logs.
		out, err := cli.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true})
		if err != nil {
			return fmt.Errorf("failed to capture container logs, %w", err)
		}

		logsBuffer := bytes.NewBuffer([]byte{})
		stdcopy.StdCopy(logsBuffer, logsBuffer, out)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{
			Force: true,
		})
		if err != nil {
			err = fmt.Errorf("error occured %w, logs: \n %s", err, logsBuffer.String())
			return err
		}
		return nil
	}, nil
}
