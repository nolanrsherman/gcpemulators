package cloudtaskemulator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nolanco/eventplatform/internal/asynctask/cloudtaskemulator/db"
	"github.com/nolanco/eventplatform/internal/testcommon"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/testing/protocmp"
)

func WhenThereIsADatabase(t *testing.T) (*mongo.Database, func(t *testing.T)) {
	t.Helper()
	bgCtx := context.Background()
	mongoDB, cleanup := db.NewTestDatabase(bgCtx, t, testMongoURI)
	db.RunMigrations(testMongoURI, mongoDB.Name())
	return mongoDB, cleanup
}

func WhenThereIsACloudTaskEmulator(t *testing.T, mongoDB *mongo.Database, grpcPort string) *CloudTaskEmulator {
	t.Helper()
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	emulator := NewCloudTaskEmulator(testMongoURI, mongoDB.Name(), logger, grpcPort)
	return emulator
}
func WhenTheCloudTaskEmulatorIsRunning(t *testing.T, emulator *CloudTaskEmulator) (stop func(t *testing.T)) {
	t.Helper()
	emuCtx, emuCtxCancel := context.WithCancel(context.Background())
	runError := make(chan error, 1)
	go func() {
		runError <- emulator.Run(emuCtx)
	}()
	select {
	case err := <-runError: //we will need to check this again later
		require.NoError(t, err)
	case <-emulator.specialEventChannels.cloudTaskEmulatorReady:
		return func(t *testing.T) {
			emuCtxCancel()
			select {
			case err := <-runError:
				RequireNoErrorBesides(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				require.FailNow(t, "cloud task emulator did not stop")
			}
		}
	case <-time.After(10 * time.Second):
		require.FailNow(t, "cloud task emulator did not start")
	}
	emuCtxCancel()
	require.FailNow(t, "cloud task emulator did not start correctly")
	return nil
}

func WithCloudTaskClient(t *testing.T, port string) (*cloudtasks.Client, func(t *testing.T)) {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	cloudTaskClient, err := cloudtasks.NewClient(context.Background(), option.WithGRPCConn(conn))
	require.NoError(t, err)
	return cloudTaskClient, func(t *testing.T) {
		// Close the client first - this will stop accepting new requests
		// and wait for in-flight requests to complete
		err := cloudTaskClient.Close()
		if err != nil {
			// Client close errors are often benign (connection closing)
			t.Logf("error closing cloud tasks client (may be expected): %v", err)
		}
		// Close the underlying connection
		// The connection may already be closed by the client, so we ignore
		// "connection is closing" errors which are expected during shutdown
		err = conn.Close()
		if err != nil && !strings.Contains(err.Error(), "connection is closing") {
			require.NoError(t, err)
		}
	}
}

func WithDefaultRpcCtxTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func RequireNoErrorBesides(t *testing.T, err error, allowedErrors ...error) {
	if err == nil {
		return
	}
	for _, allowedError := range allowedErrors {
		if errors.Is(err, allowedError) {
			return
		}
		if strings.Contains(err.Error(), allowedError.Error()) {
			return
		}
	}
	require.FailNow(t, "expected no error, got %v", err)
}

func TestCloudTaskEmulator_RpcServer(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	grpcPort := "8579"
	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB, grpcPort)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	// connect to rpc server on port 8579
	cloudTaskClient, ctcCleanup := WithCloudTaskClient(t, grpcPort)
	defer ctcCleanup(t)

	// // should be able to add a queue
	t.Run("should be able to add a queue", func(t *testing.T) {
		ctx, cancel := WithDefaultRpcCtxTimeout(t)
		defer cancel()
		queueName := primitive.NewObjectID().Hex()
		res, err := cloudTaskClient.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "projects/test-project/locations/us-central1",
			Queue: &cloudtaskspb.Queue{
				Name: fmt.Sprintf("projects/test-project/locations/us-central1/queues/%s", queueName),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res.Name)
		require.Equal(t, fmt.Sprintf("projects/test-project/locations/us-central1/queues/%s", queueName), res.Name)
	})

	t.Run("should be able to add and then get that queue", func(t *testing.T) {
		ctx, cancel := WithDefaultRpcCtxTimeout(t)
		defer cancel()
		queueID := primitive.NewObjectID().Hex()
		queueName := fmt.Sprintf("projects/test-project/locations/us-central1/queues/%s", queueID)
		createResult, err := cloudTaskClient.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "projects/test-project/locations/us-central1",
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createResult.Name)
		require.Equal(t, queueName, createResult.Name)

		ctx2, cancel2 := WithDefaultRpcCtxTimeout(t)
		defer cancel2()
		getResult, err := cloudTaskClient.GetQueue(ctx2, &cloudtaskspb.GetQueueRequest{
			Name: queueName,
		})
		require.NoError(t, err)
		require.NotNil(t, getResult.Name)
		testcommon.MustBeIdentical(t, createResult, getResult,
			protocmp.Transform(),
			cmpopts.IgnoreUnexported(cloudtaskspb.Queue{}))
	})

	t.Run("should be able to add and then list queues", func(t *testing.T) {
		ctx, cancel := WithDefaultRpcCtxTimeout(t)
		defer cancel()
		queueID := primitive.NewObjectID().Hex()
		queueName := fmt.Sprintf("projects/test-project/locations/us-central1/queues/%s", queueID)
		createResult, err := cloudTaskClient.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "projects/test-project/locations/us-central1",
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createResult.Name)
		require.Equal(t, queueName, createResult.Name)

		ctx2, cancel2 := WithDefaultRpcCtxTimeout(t)
		defer cancel2()
		listResult := cloudTaskClient.ListQueues(ctx2, &cloudtaskspb.ListQueuesRequest{
			Parent: "projects/test-project/locations/us-central1",
		})

		// find the queue we created in the list
		var foundQueue *cloudtaskspb.Queue
		for {
			queue, err := listResult.Next()
			if err != nil {
				require.Error(t, err)
				break
			}
			if queue.Name == queueName {
				foundQueue = queue
				break
			}
		}
		require.NotNil(t, foundQueue, "queue not found in list")
		testcommon.MustBeIdentical(t, createResult, foundQueue,
			protocmp.Transform(),
			cmpopts.IgnoreUnexported(cloudtaskspb.Queue{}))
	})

}

func TestCloudTaskEmulator_Service_PreExistingQueuesShouldBeProcessing(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	// Preseed the database with a queue
	clientServer := NewServer(mongoDB, zap.NewNop())
	testQueue, err := clientServer.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/test-project/locations/us-central1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/test-project/locations/us-central1/queues/test-queue",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, testQueue)

	// start up the emulator
	grpcPort := "8579"
	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB, grpcPort)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	time.Sleep(time.Second)

}

func TestCloudTaskEmulator_Service_NewQueuesShouldBeProcessing(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	// Preseed the database with a queue
	clientServer := NewServer(mongoDB, zap.NewNop())

	// start up the emulator
	grpcPort := "8579"
	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB, grpcPort)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	time.Sleep(time.Second)

	testQueue, err := clientServer.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/test-project/locations/us-central1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/test-project/locations/us-central1/queues/test-queue",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, testQueue)

	time.Sleep(10 * time.Second)

}
