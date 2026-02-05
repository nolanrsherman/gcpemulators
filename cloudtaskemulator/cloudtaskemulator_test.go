package cloudtaskemulator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nolanrsherman/gcpemulators/gcpemulators/gcpemulatorspb"
	"github.com/nolanrsherman/gcpemulators/internal/testcommon"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"
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

func WhenThereIsACloudTaskEmulator(t *testing.T, mongoDB *mongo.Database) *CloudTaskEmulator {
	t.Helper()
	port, err := findFreePort()
	require.NoError(t, err)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	emulator := NewCloudTaskEmulator(testMongoURI, mongoDB.Name(), logger, port)
	return emulator
}

// findFreePort returns an available port by binding to port 0 and then closing the listener.
// This uses the standard library approach: net.Listen with port 0 lets the OS assign a free port.
func findFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
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
		require.NoError(t, err, "cloud task finished with error")
	case <-time.After(time.Millisecond * 100):
		return func(t *testing.T) {
			emuCtxCancel()
			select {
			case err := <-runError:
				RequireNoErrorBesides(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				require.FailNow(t, "cloud task emulator did not stop")
			}
		}
	}
	emuCtxCancel()
	require.FailNow(t, "cloud task emulator did not start correctly")
	return nil
}

func WithCloudTaskClient(t *testing.T, port int) (*cloudtasks.Client, func(t *testing.T)) {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func TestCloudTaskEmulatorService_RpcServer(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	// connect to rpc server on port 8579
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", emulator.port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	cloudTaskClient := gcpemulatorspb.NewGcpEmulatorClient(conn)

	readiness, err := cloudTaskClient.Readiness(context.Background(), &gcpemulatorspb.ReadinessRequest{})
	require.NoError(t, err)
	require.True(t, readiness.Ready)
}

func TestCloudTaskEmulator_RpcServer(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	// connect to rpc server on port 8579
	cloudTaskClient, ctcCleanup := WithCloudTaskClient(t, emulator.port)
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

func TestCloudTaskEmulator_Service_PreExistingQueueAndTaskShouldBeProcessed(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	// Pre-seed the database with a queue and task
	emulator := NewCloudTaskEmulator(testMongoURI, mongoDB.Name(), zap.NewNop(), 0)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)

	testQueue := WhenAQueueIsCreated(t, emulator, mongoDB)
	testTask := WhenAHttpTargetTaskIsCreated(t, emulator, mongoDB, testQueue, &cloudtaskspb.HttpRequest{
		Url: "http://localhost:9999/test",
	})
	require.NotNil(t, testQueue)
	require.NotNil(t, testTask)
	stopEmulator(t)

	// start up the emulator
	stopEmulator = WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	require.Eventually(t, func() bool {
		task, err := db.SelectTaskByNameIncludingDeleted(context.Background(), mongoDB, testTask.Name)
		require.NoError(t, err)
		require.NotNil(t, task)
		return task.DispatchCount > 0
	}, 10*time.Second, 100*time.Millisecond, "task should be dispatched")

}

func TestCloudTaskEmulator_Service_NewQueuesShouldBeProcessing(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	// start up the emulator
	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	// connect to emulator with grpc
	cloudTaskClient, ctcCleanup := WithCloudTaskClient(t, emulator.port)
	defer ctcCleanup(t)

	queue, err := cloudTaskClient.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/test-project/locations/us-central1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/test-project/locations/us-central1/queues/test-queue",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, queue)

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()
	task, err := cloudTaskClient.CreateTask(context.Background(), &cloudtaskspb.CreateTaskRequest{
		Parent: queue.Name,
		Task: &cloudtaskspb.Task{
			Name: queue.Name + "/tasks/test-task",
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					Url:        testServer.URL,
					HttpMethod: cloudtaskspb.HttpMethod_POST,
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, task)

	require.Eventually(t, func() bool {
		task, err := db.SelectTaskByNameIncludingDeleted(context.Background(), mongoDB, task.Name)
		require.NoError(t, err)
		require.NotNil(t, task)
		return task.DispatchCount > 0
	}, 5*time.Second, 100*time.Millisecond, "task should be dispatched")

	taskAfterRun, err := db.SelectTaskByNameIncludingDeleted(context.Background(), mongoDB, task.Name)
	require.NoError(t, err)
	require.NotNil(t, taskAfterRun)
}

func TestCloudTaskEmulator_ManyQueuesManyTasks(t *testing.T) {
	mongoDB, dbCleanup := WhenThereIsADatabase(t)
	defer dbCleanup(t)

	// start up the emulator
	emulator := WhenThereIsACloudTaskEmulator(t, mongoDB)
	stopEmulator := WhenTheCloudTaskEmulatorIsRunning(t, emulator)
	defer stopEmulator(t)

	// connect to emulator with grpc
	cloudTaskClient, ctcCleanup := WithCloudTaskClient(t, emulator.port)
	defer ctcCleanup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueNames := make([]string, 10)
	for i := 0; i < 10; i++ {
		queue, err := cloudTaskClient.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "projects/test-project/locations/us-central1",
			Queue: &cloudtaskspb.Queue{
				Name: fmt.Sprintf("projects/test-project/locations/us-central1/queues/test-queue-%d", i),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, queue)
		queueNames[i] = queue.Name
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	taskChannel := make(chan *cloudtaskspb.Task, 100)
	go func() {
		wg := sync.WaitGroup{}
		for _, queueName := range queueNames {
			wg.Add(1)
			go func(t *testing.T) {
				t.Helper()
				defer wg.Done()
				for i := 0; i < 50; i++ {
					select {
					case <-ctx.Done():
						return
					default:
					}
					task, err := cloudTaskClient.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
						Parent: queueName,
						Task: &cloudtaskspb.Task{
							MessageType: &cloudtaskspb.Task_HttpRequest{
								HttpRequest: &cloudtaskspb.HttpRequest{
									Url: testServer.URL,
								},
							},
						},
					})
					require.NoError(t, err)
					taskChannel <- task
				}
			}(t)
		}
		wg.Wait()
		close(taskChannel)
	}()

	for task := range taskChannel {
		require.NotNil(t, task)
		require.Eventually(t, func() bool {
			task, err := db.SelectTaskByNameIncludingDeleted(context.Background(), mongoDB, task.Name)
			require.NoError(t, err)
			require.NotNil(t, task)
			return task.DispatchCount > 0
		}, 5*time.Second, 100*time.Millisecond, "task should be dispatched")

		taskAfterRun, err := db.SelectTaskByNameIncludingDeleted(context.Background(), mongoDB, task.Name)
		require.NoError(t, err)
		require.NotNil(t, taskAfterRun)
		require.Equal(t, db.TaskStatusSucceeded, taskAfterRun.Status)
	}

}
