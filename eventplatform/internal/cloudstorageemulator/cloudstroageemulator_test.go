package cloudstorageemulator

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// StartDockerizedCloudStorageEmulator builds and runs the cloud storage emulator in a Docker container.
// It returns the port the emulator is running on and a cleanup function to stop and remove the container.
func StartDockerizedCloudStorageEmulator(t *testing.T, port int) (int, func()) {
	t.Helper()

	// Get the directory containing the dockerfile (this is the module root)
	_, filename, _, _ := runtime.Caller(0)
	dockerfileDir := filepath.Dir(filename)

	// Generate unique image and container names
	imageName := fmt.Sprintf("cloudstorageemulator-test-%d", time.Now().UnixNano())
	containerName := fmt.Sprintf("cloudstorageemulator-test-%d", time.Now().UnixNano())

	// Build the Docker image
	// Build context is the directory containing dockerfile, and we specify the dockerfile explicitly
	t.Logf("Building Docker image: %s from %s", imageName, dockerfileDir)
	buildCmd := exec.Command("docker", "build",
		"-f", filepath.Join(dockerfileDir, "dockerfile"),
		"-t", imageName,
		dockerfileDir)
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build Docker image: %v", err)
	}

	// Run the container
	t.Logf("Starting Docker container: %s on port %d", containerName, port)
	runCmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"-p", fmt.Sprintf("%d:%d", port, port),
		imageName,
		"--port", strconv.Itoa(port))

	var runOutput bytes.Buffer
	runCmd.Stdout = &runOutput
	runCmd.Stderr = os.Stderr

	if err := runCmd.Run(); err != nil {
		// Clean up image if container creation fails
		exec.Command("docker", "rmi", imageName).Run()
		t.Fatalf("failed to run Docker container: %v", err)
	}
	containerID := strings.TrimSpace(runOutput.String())

	// Wait for container to be ready by checking if it's running and the port is listening
	t.Logf("Waiting for container to be ready...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			// Clean up on timeout
			exec.Command("docker", "stop", containerID).Run()
			exec.Command("docker", "rm", containerID).Run()
			exec.Command("docker", "rmi", imageName).Run()
			t.Fatalf("container failed to start within timeout")
		default:
			// Check if container is running
			checkCmd := exec.Command("docker", "ps", "--filter", fmt.Sprintf("id=%s", containerID), "--format", "{{.Status}}")
			output, err := checkCmd.Output()
			if err == nil && strings.Contains(string(output), "Up") {
				// Try to connect to the port to verify it's actually listening
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 100*time.Millisecond)
				if err == nil {
					conn.Close()
					t.Logf("Container is ready on port %d", port)
					return port, func() {
						t.Helper()
						t.Logf("Stopping and removing container: %s", containerName)
						exec.Command("docker", "stop", containerID).Run()
						exec.Command("docker", "rm", containerID).Run()
						exec.Command("docker", "rmi", imageName).Run()
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func FindOpenPort(t *testing.T) int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to find open port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
func TestCloudStorageEmulatorStartAndStop(t *testing.T) {
	port := FindOpenPort(t)
	port, cleanup := StartDockerizedCloudStorageEmulator(t, port)
	defer cleanup()
}

func TestCloudStorageEmulatorCreateBucket(t *testing.T) {
	port := FindOpenPort(t)
	storageGrpcConn, err := grpc.NewClient("localhost:"+strconv.Itoa(port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	storageClient, err := storage.NewGRPCClient(ctx, option.WithGRPCConn(storageGrpcConn))
	require.NoError(t, err)

	bucketName := "test-bucket"
	projectID := "test-project"
	bucket := storageClient.Bucket(bucketName).Create(ctx, projectID, &storage.BucketAttrs{
		Name: bucketName,
	})
	require.NoError(t, err)
	require.NotNil(t, bucket)

}
