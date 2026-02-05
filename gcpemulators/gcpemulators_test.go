package gcpemulators

import (
	"context"
	"fmt"
	"testing"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nolanrsherman/gcpemulators/gcpemulators/gcpemulatorspb"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewCloudTaskEmulator(t *testing.T) {
	emulator, cleanup, err := NewCloudTaskEmulator()
	require.NoError(t, err)
	defer func(t *testing.T) {
		t.Helper()
		err := cleanup()
		require.NoError(t, err)
	}(t)
	require.NotNil(t, emulator)
	require.NotNil(t, cleanup)

	readiness, err := emulator.Client.Readiness(context.Background(), &gcpemulatorspb.ReadinessRequest{})
	require.NoError(t, err)
	require.True(t, readiness.Ready)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", emulator.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	// initialize the cloud task client
	cloudTaskClient, err := cloudtasks.NewClient(context.Background(), option.WithGRPCConn(conn))
	require.NoError(t, err)

	t.Run("should be able to create a queue", func(t *testing.T) {
		q, err := cloudTaskClient.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
			Parent: "projects/12345/locations/us-central1",
			Queue: &cloudtaskspb.Queue{
				Name: "projects/12345/locations/us-central1/queues/" + primitive.NewObjectID().Hex(),
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts: 3,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, q)
	})

}

func TestNewCloudStorageEmulator(t *testing.T) {
	emulator, cleanup, err := NewCloudStorageEmulator()
	require.NoError(t, err)
	defer func(t *testing.T) {
		t.Helper()
		err := cleanup()
		require.NoError(t, err)
	}(t)
	require.NotNil(t, emulator)
	require.NotNil(t, cleanup)

	readiness, err := emulator.Client.Readiness(context.Background(), &gcpemulatorspb.ReadinessRequest{})
	require.NoError(t, err)
	require.True(t, readiness.Ready)

}
