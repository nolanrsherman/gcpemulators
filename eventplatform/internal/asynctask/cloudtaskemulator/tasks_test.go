package cloudtaskemulator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/nolanco/eventplatform/internal/asynctask/cloudtaskemulator/db"
	"github.com/nolanco/eventplatform/internal/testcommon"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreateTask(t *testing.T) {
	ctx := context.Background()
	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"
	queueName := parent + "/queues/test-queue"

	// Create a queue for testing
	_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
		Parent: parent,
		Queue: &cloudtaskspb.Queue{
			Name: queueName,
		},
	})
	require.NoError(t, err)

	t.Run("should reject empty parent", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: "",
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate parent name format", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: "invalid-parent",
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should return NotFound for non-existent queue", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: parent + "/queues/does-not-exist",
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("should reject nil task", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task:   nil,
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate task name format when provided", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: "bad-name",
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject task name that does not match parent prefix", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: parent + "/queues/other-queue/tasks/task1",
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject AppEngineHttpRequest", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_AppEngineHttpRequest{
					AppEngineHttpRequest: &cloudtaskspb.AppEngineHttpRequest{},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject missing http_request", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task:   &cloudtaskspb.Task{},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject empty URL", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject URL that does not start with http:// or https://", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "ftp://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject invalid URL format", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://[invalid-url",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject dispatch deadline less than 15 seconds", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				DispatchDeadline: durationpb.New(10 * time.Second),
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject dispatch deadline greater than 30 minutes", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				DispatchDeadline: durationpb.New(31 * time.Minute),
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject body with GET method", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_GET,
						Body:       []byte("body"),
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject body with DELETE method", func(t *testing.T) {
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_DELETE,
						Body:       []byte("body"),
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject task size exceeding 100KB", func(t *testing.T) {
		largeBody := make([]byte, 100*1024+1) // 100KB + 1 byte
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:  "https://example.com",
						Body: largeBody,
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should generate task name when not provided", func(t *testing.T) {
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res.GetName())
		require.Contains(t, res.GetName(), queueName+"/tasks/")
		require.Len(t, res.GetName(), len(queueName)+len("/tasks/")+24) // 24 is ObjectID hex length
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:     timestamppb.New(time.Now()),
			CreateTime:       timestamppb.New(time.Now()),
			DispatchCount:    0,
			ResponseCount:    0,
			View:             cloudtaskspb.Task_BASIC,
			DispatchDeadline: nil,
			FirstAttempt:     nil,
			LastAttempt:      nil,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
		require.WithinDuration(t, expectedTask.CreateTime.AsTime(), res.CreateTime.AsTime(), 1*time.Second)
		require.WithinDuration(t, expectedTask.ScheduleTime.AsTime(), res.ScheduleTime.AsTime(), 1*time.Second)
	})

	t.Run("should accept valid task name when provided", func(t *testing.T) {
		taskName := queueName + "/tasks/test-task-123"
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, taskName, res.GetName())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: taskName,
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_BASIC,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
		require.WithinDuration(t, expectedTask.CreateTime.AsTime(), res.CreateTime.AsTime(), 1*time.Second)
		require.WithinDuration(t, expectedTask.ScheduleTime.AsTime(), res.ScheduleTime.AsTime(), 1*time.Second)
	})

	t.Run("should set schedule_time to now if in the past", func(t *testing.T) {
		pastTime := time.Now().Add(-1 * time.Hour)
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				ScheduleTime: timestamppb.New(pastTime),
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res.GetScheduleTime())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_BASIC,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
		// Schedule time should be approximately now (within 1 second)
		require.WithinDuration(t, now, res.GetScheduleTime().AsTime(), 1*time.Second)
	})

	t.Run("should default HttpMethod to POST when unspecified", func(t *testing.T) {
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED,
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res.GetHttpRequest())
		require.Equal(t, cloudtaskspb.HttpMethod_POST, res.GetHttpRequest().GetHttpMethod())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_BASIC,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
	})

	t.Run("should accept valid dispatch deadline", func(t *testing.T) {
		deadline := 20 * time.Minute
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				DispatchDeadline: durationpb.New(deadline),
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res.GetDispatchDeadline())
		require.Equal(t, deadline, res.GetDispatchDeadline().AsDuration())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:     timestamppb.New(now),
			CreateTime:       timestamppb.New(now),
			DispatchCount:    0,
			ResponseCount:    0,
			View:             cloudtaskspb.Task_BASIC,
			DispatchDeadline: durationpb.New(deadline),
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
	})

	t.Run("should accept body with POST method", func(t *testing.T) {
		body := []byte("test body")
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_POST,
						Body:       body,
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res.GetHttpRequest())
		require.Equal(t, body, res.GetHttpRequest().GetBody())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
					Body:       body,
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_BASIC,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
	})

	t.Run("should persist task to database", func(t *testing.T) {
		taskName := queueName + "/tasks/persist-test"
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_PUT,
						Headers:    map[string]string{"Content-Type": "application/json"},
						Body:       []byte(`{"key": "value"}`),
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, taskName, res.GetName())

		// Verify task was persisted
		col := mongoDB.Collection(db.CollectionTasks)
		var dbTask db.Task
		err = col.FindOne(ctx, bson.M{"name": taskName}).Decode(&dbTask)
		require.NoError(t, err)
		require.Equal(t, taskName, dbTask.Name)
		require.Equal(t, db.MessageTypeHttpRequest, dbTask.MessageType)
		require.NotNil(t, dbTask.HttpRequest)
		require.Equal(t, "https://example.com", dbTask.HttpRequest.Url)
		require.Equal(t, cloudtaskspb.HttpMethod_PUT, dbTask.HttpRequest.HttpMethod)
		require.Equal(t, map[string]string{"Content-Type": "application/json"}, dbTask.HttpRequest.Headers)
		require.Equal(t, []byte(`{"key": "value"}`), dbTask.HttpRequest.Body)
	})

	t.Run("should return AlreadyExists for duplicate task name", func(t *testing.T) {
		taskName := queueName + "/tasks/duplicate-test"
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		// Try to create the same task again
		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.AlreadyExists, st.Code())
	})

	t.Run("should set default view to BASIC when unspecified", func(t *testing.T) {
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
			ResponseView: cloudtaskspb.Task_VIEW_UNSPECIFIED,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Task_BASIC, res.GetView())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_BASIC,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
	})

	t.Run("should accept FULL view", func(t *testing.T) {
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Task_FULL, res.GetView())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_FULL,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
	})

	t.Run("should set output-only fields correctly", func(t *testing.T) {
		res, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, int32(0), res.GetDispatchCount())
		require.Equal(t, int32(0), res.GetResponseCount())
		require.NotNil(t, res.GetCreateTime())

		now := time.Now()
		expectedTask := &cloudtaskspb.Task{
			Name: res.GetName(),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: &cloudtaskspb.HttpRequest{
					HttpMethod: cloudtaskspb.HttpMethod_POST,
					Url:        "https://example.com",
				},
			},
			ScheduleTime:  timestamppb.New(now),
			CreateTime:    timestamppb.New(now),
			DispatchCount: 0,
			ResponseCount: 0,
			View:          cloudtaskspb.Task_BASIC,
		}
		testcommon.MustBeIdentical(t, expectedTask, res,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
		)
	})
}

func TestGetTask(t *testing.T) {
	ctx := context.Background()
	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"
	queueName := parent + "/queues/test-queue"

	// Create a queue for testing
	_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
		Parent: parent,
		Queue: &cloudtaskspb.Queue{
			Name: queueName,
		},
	})
	require.NoError(t, err)

	t.Run("should reject empty task name", func(t *testing.T) {
		_, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate task name format", func(t *testing.T) {
		_, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: "invalid-task-name",
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should return NotFound for non-existent task", func(t *testing.T) {
		_, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: queueName + "/tasks/does-not-exist",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("should return task when it exists", func(t *testing.T) {
		// First create a task
		taskName := queueName + "/tasks/get-test-task"
		createRes, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com/get",
						HttpMethod: cloudtaskspb.HttpMethod_GET,
						Headers:    map[string]string{"X-Custom-Header": "value"},
					},
				},
				DispatchDeadline: durationpb.New(20 * time.Minute),
			},
		})
		require.NoError(t, err)
		require.Equal(t, taskName, createRes.GetName())

		// Now fetch it via GetTask
		got, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: taskName,
		})
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, taskName, got.GetName())
		require.NotNil(t, got.GetHttpRequest())
		require.Equal(t, "https://example.com/get", got.GetHttpRequest().GetUrl())
		require.Equal(t, cloudtaskspb.HttpMethod_GET, got.GetHttpRequest().GetHttpMethod())
		require.Equal(t, map[string]string{"X-Custom-Header": "value"}, got.GetHttpRequest().GetHeaders())
		require.NotNil(t, got.GetDispatchDeadline())
		require.Equal(t, 20*time.Minute, got.GetDispatchDeadline().AsDuration())
		require.NotNil(t, got.GetScheduleTime())
		require.NotNil(t, got.GetCreateTime())
		require.Equal(t, int32(0), got.GetDispatchCount())
		require.Equal(t, int32(0), got.GetResponseCount())
	})

	t.Run("should default to BASIC view when ResponseView is unspecified", func(t *testing.T) {
		taskName := queueName + "/tasks/view-test-basic"
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		got, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name:         taskName,
			ResponseView: cloudtaskspb.Task_VIEW_UNSPECIFIED,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Task_BASIC, got.GetView())
	})

	t.Run("should return BASIC view when requested", func(t *testing.T) {
		taskName := queueName + "/tasks/view-test-basic-explicit"
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		got, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name:         taskName,
			ResponseView: cloudtaskspb.Task_BASIC,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Task_BASIC, got.GetView())
	})

	t.Run("should return FULL view when requested", func(t *testing.T) {
		taskName := queueName + "/tasks/view-test-full"
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_POST,
						Body:       []byte("test body"),
					},
				},
			},
		})
		require.NoError(t, err)

		got, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name:         taskName,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Task_FULL, got.GetView())
		// FULL view should include the body
		require.NotNil(t, got.GetHttpRequest())
		require.Equal(t, []byte("test body"), got.GetHttpRequest().GetBody())
	})

	t.Run("should return task with all fields populated", func(t *testing.T) {
		taskName := queueName + "/tasks/complete-task"
		scheduleTime := time.Now().Add(1 * time.Hour)
		_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name:         taskName,
				ScheduleTime: timestamppb.New(scheduleTime),
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com/complete",
						HttpMethod: cloudtaskspb.HttpMethod_PUT,
						Headers: map[string]string{
							"Content-Type":  "application/json",
							"Authorization": "Bearer token123",
						},
						Body: []byte(`{"action": "update"}`),
					},
				},
				DispatchDeadline: durationpb.New(15 * time.Minute),
			},
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)

		got, err := srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name:         taskName,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)
		require.Equal(t, taskName, got.GetName())
		require.NotNil(t, got.GetScheduleTime())
		require.WithinDuration(t, scheduleTime, got.GetScheduleTime().AsTime(), time.Second)
		require.NotNil(t, got.GetCreateTime())
		require.NotNil(t, got.GetHttpRequest())
		require.Equal(t, "https://example.com/complete", got.GetHttpRequest().GetUrl())
		require.Equal(t, cloudtaskspb.HttpMethod_PUT, got.GetHttpRequest().GetHttpMethod())
		require.Equal(t, map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer token123",
		}, got.GetHttpRequest().GetHeaders())
		require.Equal(t, []byte(`{"action": "update"}`), got.GetHttpRequest().GetBody())
		require.NotNil(t, got.GetDispatchDeadline())
		require.Equal(t, 15*time.Minute, got.GetDispatchDeadline().AsDuration())
		require.Equal(t, cloudtaskspb.Task_FULL, got.GetView())
	})
}

func TestListTasks(t *testing.T) {
	ctx := context.Background()

	t.Run("should reject empty parent", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate parent name format", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent: "invalid-parent",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should return NotFound for non-existent queue", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent: "projects/test-project/locations/us-central1/queues/does-not-exist",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("should return tasks in lexicographical order and support pagination", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/test-queue"

		// Create a queue
		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Create three tasks with fixed names under the same queue but out of order
		create := func(taskName string) {
			_, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
				Parent: queueName,
				Task: &cloudtaskspb.Task{
					Name: taskName,
					MessageType: &cloudtaskspb.Task_HttpRequest{
						HttpRequest: &cloudtaskspb.HttpRequest{
							Url: "https://example.com",
						},
					},
				},
			})
			require.NoError(t, err)
		}

		t1 := queueName + "/tasks/task-alpha"
		t2 := queueName + "/tasks/task-gamma"
		t3 := queueName + "/tasks/task-beta"

		create(t1)
		create(t2)
		create(t3)

		// First page: page_size=2
		listRes1, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:   queueName,
			PageSize: 2,
		})
		require.NoError(t, err)
		require.Len(t, listRes1.GetTasks(), 2)

		// Tasks should be sorted lexicographically by name
		names1 := []string{listRes1.Tasks[0].GetName(), listRes1.Tasks[1].GetName()}
		require.Equal(t, []string{t1, t3}, names1)

		// Second page using next_page_token
		require.NotEmpty(t, listRes1.GetNextPageToken())
		listRes2, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:    queueName,
			PageSize:  2,
			PageToken: listRes1.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, listRes2.GetTasks(), 1)
		require.Equal(t, t2, listRes2.Tasks[0].GetName())
		require.Empty(t, listRes2.GetNextPageToken())
	})

	t.Run("should default to BASIC view when ResponseView is unspecified", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/view-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		res, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:       queueName,
			ResponseView: cloudtaskspb.Task_VIEW_UNSPECIFIED,
		})
		require.NoError(t, err)
		require.Len(t, res.GetTasks(), 1)
		require.Equal(t, cloudtaskspb.Task_BASIC, res.Tasks[0].GetView())
	})

	t.Run("should return BASIC view when requested", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/basic-view-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		res, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:       queueName,
			ResponseView: cloudtaskspb.Task_BASIC,
		})
		require.NoError(t, err)
		require.Len(t, res.GetTasks(), 1)
		require.Equal(t, cloudtaskspb.Task_BASIC, res.Tasks[0].GetView())
	})

	t.Run("should return FULL view when requested", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/full-view-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url:        "https://example.com",
						HttpMethod: cloudtaskspb.HttpMethod_POST,
						Body:       []byte("test body"),
					},
				},
			},
		})
		require.NoError(t, err)

		res, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:       queueName,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)
		require.Len(t, res.GetTasks(), 1)
		require.Equal(t, cloudtaskspb.Task_FULL, res.Tasks[0].GetView())
		// FULL view should include the body
		require.NotNil(t, res.Tasks[0].GetHttpRequest())
		require.Equal(t, []byte("test body"), res.Tasks[0].GetHttpRequest().GetBody())
	})

	t.Run("should clamp page size to maximum of 1000", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/page-size-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Request page size larger than maximum
		res, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:   queueName,
			PageSize: 2000,
		})
		require.NoError(t, err)
		// Should still work, but internally clamped to 1000
		require.NotNil(t, res)
	})

	t.Run("should default page size to 1000 when unspecified", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/default-page-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Request with page_size=0 (unspecified)
		res, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:   queueName,
			PageSize: 0,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		// Should default to 1000 internally
	})

	t.Run("should reject invalid page_token", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/invalid-token-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		_, err = srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent:    queueName,
			PageToken: "invalid-token",
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should return empty list when queue has no tasks", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/empty-queue"

		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		res, err := srv.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent: queueName,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, res.GetTasks(), 0)
		require.Empty(t, res.GetNextPageToken())
	})
}

func TestDeleteTask(t *testing.T) {
	ctx := context.Background()

	t.Run("should reject empty task name", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate task name format", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: "invalid-task-name",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should return NotFound for non-existent task", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/test-queue"

		// Create a queue for testing
		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: queueName + "/tasks/does-not-exist",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("should return FailedPrecondition for succeeded task", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/test-queue"

		// Create a queue for testing
		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Create a task
		taskName := queueName + "/tasks/succeeded-task"
		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		// Update task status to succeeded
		col := mongoDB.Collection(db.CollectionTasks)
		_, err = col.UpdateOne(ctx, bson.M{"name": taskName}, bson.M{
			"$set": bson.M{
				"status": db.TaskStatusSucceeded,
			},
		})
		require.NoError(t, err)

		// Try to delete the succeeded task
		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, st.Code())
		require.Contains(t, st.Message(), "cannot be deleted because it has already succeeded or permanently failed")
	})

	t.Run("should return FailedPrecondition for failed task", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/test-queue"

		// Create a queue for testing
		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Create a task
		taskName := queueName + "/tasks/failed-task"
		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		// Update task status to failed
		col := mongoDB.Collection(db.CollectionTasks)
		_, err = col.UpdateOne(ctx, bson.M{"name": taskName}, bson.M{
			"$set": bson.M{
				"status": db.TaskStatusFailed,
			},
		})
		require.NoError(t, err)

		// Try to delete the failed task
		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.FailedPrecondition, st.Code())
		require.Contains(t, st.Message(), "cannot be deleted because it has already succeeded or permanently failed")
	})

	t.Run("should successfully delete pending task", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/test-queue"

		// Create a queue for testing
		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Create a task
		taskName := queueName + "/tasks/pending-task"
		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify task exists before deletion
		_, err = srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: taskName,
		})
		require.NoError(t, err)

		// Delete the task
		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: taskName,
		})
		require.NoError(t, err)

		// Verify task is soft-deleted (cannot be retrieved via GetTask)
		_, err = srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())

		// Verify task still exists in database with deleted_at set
		col := mongoDB.Collection(db.CollectionTasks)
		var dbTask db.Task
		err = col.FindOne(ctx, bson.M{"name": taskName}).Decode(&dbTask)
		require.NoError(t, err)
		require.NotNil(t, dbTask.DeletedAt)
		require.WithinDuration(t, time.Now(), *dbTask.DeletedAt, 5*time.Second)
	})

	t.Run("should successfully delete running task", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"
		queueName := parent + "/queues/test-queue"

		// Create a queue for testing
		_, err = srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)

		// Create a task
		taskName := queueName + "/tasks/running-task"
		_, err = srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: queueName,
			Task: &cloudtaskspb.Task{
				Name: taskName,
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: "https://example.com",
					},
				},
			},
		})
		require.NoError(t, err)

		// Update task status to running
		col := mongoDB.Collection(db.CollectionTasks)
		_, err = col.UpdateOne(ctx, bson.M{"name": taskName}, bson.M{
			"$set": bson.M{
				"status": db.TaskStatusRunning,
			},
		})
		require.NoError(t, err)

		// Delete the running task
		_, err = srv.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{
			Name: taskName,
		})
		require.NoError(t, err)

		// Verify task is soft-deleted (cannot be retrieved via GetTask)
		_, err = srv.GetTask(ctx, &cloudtaskspb.GetTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())

		// Verify task still exists in database with deleted_at set
		var dbTask db.Task
		err = col.FindOne(ctx, bson.M{"name": taskName}).Decode(&dbTask)
		require.NoError(t, err)
		require.NotNil(t, dbTask.DeletedAt)
		require.WithinDuration(t, time.Now(), *dbTask.DeletedAt, 5*time.Second)
	})
}

func TestRunTask(t *testing.T) {
	ctx := context.Background()

	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	srv := NewServer(mongoDB, logger)

	// - should reject empty task name (InvalidArgument)
	t.Run("should reject empty task name", func(t *testing.T) {
		_, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	// - should validate task name format (InvalidArgument for invalid name)
	t.Run("should validate task name format", func(t *testing.T) {
		_, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name: "invalid-task-name",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	// - should return NotFound when task does not exist
	t.Run("should return NotFound when task does not exist", func(t *testing.T) {
		taskName := "projects/test-project/locations/us-central1/queues/test-queue/tasks/does-not-exist"
		_, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	// - should return NotFound when task has already succeeded (soft-deleted by processTask)
	t.Run("should return NotFound when task has already succeeded", func(t *testing.T) {
		taskName := "projects/test-project/locations/us-central1/queues/test-queue/tasks/succeeded-task"

		// Insert a task with status Succeeded (no deleted_at so SelectTaskByName can find it)
		col := mongoDB.Collection(db.CollectionTasks)
		_, err := col.InsertOne(ctx, &db.Task{
			Name:   taskName,
			Status: db.TaskStatusSucceeded,
		})
		require.NoError(t, err)

		_, err = srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	// - should return NotFound when task has already permanently failed (soft-deleted by processTask)
	t.Run("should return NotFound when task has already permanently failed", func(t *testing.T) {
		taskName := "projects/test-project/locations/us-central1/queues/test-queue/tasks/failed-task"

		// Insert a task with status Failed (no deleted_at so SelectTaskByName can find it)
		col := mongoDB.Collection(db.CollectionTasks)
		_, err := col.InsertOne(ctx, &db.Task{
			Name:   taskName,
			Status: db.TaskStatusFailed,
		})
		require.NoError(t, err)

		_, err = srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name: taskName,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("should run a pending task successfully", func(t *testing.T) {
		// - create a test http server to assign as the target to a task. It should return 200 OK.
		responseReceivedChan := make(chan struct{})
		var responseReceivedTime time.Time
		var receivedRequest *http.Request
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(responseReceivedChan)
			receivedRequest = r
			responseReceivedTime = time.Now()
			w.WriteHeader(http.StatusOK)
		}))
		defer testServer.Close()

		// - create a queue
		queueCustomName := primitive.NewObjectID().Hex()
		queueName := "projects/test-project/locations/us-central1/queues/" + queueCustomName
		createdCloudQueue, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "projects/test-project/locations/us-central1",
			Queue: &cloudtaskspb.Queue{
				Name: queueName,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createdCloudQueue)

		dbCreatedQueue, err := db.SelectQueueByName(ctx, mongoDB, createdCloudQueue.Name)
		require.NoError(t, err)
		require.NotNil(t, dbCreatedQueue)

		// - create a createdCloudTask to run against the test server.
		createdCloudTask, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
			Parent: createdCloudQueue.Name,
			Task: &cloudtaskspb.Task{
				Name: createdCloudQueue.Name + "/tasks/" + primitive.NewObjectID().Hex(),
				MessageType: &cloudtaskspb.Task_HttpRequest{
					HttpRequest: &cloudtaskspb.HttpRequest{
						Url: testServer.URL,
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, createdCloudTask)

		dbCreatedTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, createdCloudTask.Name)
		require.NoError(t, err)
		require.NotNil(t, dbCreatedTask)

		//   - call RunTask and expect:
		taskStateBeforeRun, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name: createdCloudTask.Name,
		})
		require.NoError(t, err)
		require.NotNil(t, taskStateBeforeRun)
		testcommon.MustBeIdentical(t, createdCloudTask, taskStateBeforeRun,
			protocmp.Transform(),
			protocmp.IgnoreFields(&cloudtaskspb.Task{}, "create_time", "schedule_time"),
			cmpopts.EquateApproxTime(time.Second),
		)
		require.WithinDuration(t, time.Now(), taskStateBeforeRun.CreateTime.AsTime(), 5*time.Second)
		require.WithinDuration(t, time.Now(), taskStateBeforeRun.ScheduleTime.AsTime(), 5*time.Second)

		select {
		case <-responseReceivedChan:
		case <-time.After(time.Second):
			require.FailNow(t, "response not received")
		}

		// validate the request was as expected
		require.NotNil(t, receivedRequest)
		require.Equal(t, http.MethodPost, receivedRequest.Method)
		// Validate the body was empty
		require.Empty(t, receivedRequest.Body)
		// validate the headers were empty
		testcommon.MustBeIdentical(t, http.Header{
			"Accept-Encoding": {"gzip"},
			"Content-Length":  {"0"},
			"User-Agent":      {"Go-http-client/1.1"},
		}, receivedRequest.Header)

		require.Eventually(t, func() bool {
			actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, createdCloudTask.Name)
			require.NoError(t, err)
			require.NotNil(t, actualTask)

			return actualTask.Status == db.TaskStatusSucceeded
		}, 10*time.Second, 100*time.Millisecond, "task should eventually become succeeded")

		actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, createdCloudTask.Name)
		expectedTask := &db.Task{
			Id:            dbCreatedTask.Id,
			Name:          createdCloudTask.Name,
			CreatedAt:     taskStateBeforeRun.CreateTime.AsTime(),
			UpdatedAt:     responseReceivedTime.UTC(),
			DeletedAt:     testcommon.Ptr(responseReceivedTime.UTC()),
			QueueID:       dbCreatedQueue.Id,
			LockExpiresAt: nil,
			MessageType:   db.MessageTypeHttpRequest,
			HttpRequest: &db.Task_HttpRequest{
				Url:        testServer.URL,
				HttpMethod: cloudtaskspb.HttpMethod_POST,
				Headers:    nil,
				Body:       nil,
			},
			ScheduleTime:     testcommon.Ptr(taskStateBeforeRun.ScheduleTime.AsTime()),
			CreateTime:       testcommon.Ptr(taskStateBeforeRun.CreateTime.AsTime()),
			DispatchDeadline: nil,
			Status:           db.TaskStatusSucceeded,
			DispatchCount:    1,
			ResponseCount:    1,
			FirstAttempt: &db.Task_Attempt{
				ScheduleTime:          testcommon.Ptr(taskStateBeforeRun.ScheduleTime.AsTime()),
				DispatchTime:          testcommon.Ptr(responseReceivedTime.UTC()),
				ResponseTime:          testcommon.Ptr(responseReceivedTime.UTC()),
				ResponseStatusCode:    int32(codes.OK),
				ResponseStatusMessage: "HTTP request executed successfully",
			},
			LastAttempt: &db.Task_Attempt{
				ScheduleTime:          testcommon.Ptr(taskStateBeforeRun.ScheduleTime.AsTime()),
				DispatchTime:          testcommon.Ptr(responseReceivedTime.UTC()),
				ResponseTime:          testcommon.Ptr(responseReceivedTime.UTC()),
				ResponseStatusCode:    int32(codes.OK),
				ResponseStatusMessage: "HTTP request executed successfully",
			},
		}
		testcommon.MustBeIdentical(t, expectedTask, actualTask, cmpopts.EquateApproxTime(time.Second))
	})

	t.Run("should handle non-2xx HTTP responses as retryable or non-retryable correctly", func(t *testing.T) {

		type TestCase struct {
			testName                  string
			workerResponseStatusCode  int
			shouldPermanentlyFail     bool
			expectedAttemptStatusCode codes.Code
		}

		testFn := func(t *testing.T, testCase TestCase) {
			t.Helper()
			queue := WhenAQueueIsCreated(t, srv, mongoDB, CreateQueueWithUnlimitedRetries())

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(testCase.workerResponseStatusCode)
			}))
			task := WhenAHttpTargetTaskIsCreated(t, srv, mongoDB, queue, &cloudtaskspb.HttpRequest{
				Url:        testServer.URL,
				HttpMethod: cloudtaskspb.HttpMethod_POST,
				Headers:    nil,
				Body:       nil,
			})

			defer testServer.Close()

			taskStateBeforeRun, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
				Name:         task.Name,
				ResponseView: cloudtaskspb.Task_FULL,
			})
			require.NoError(t, err)
			require.NotNil(t, taskStateBeforeRun)
			expectedTaskState := task.ToCloudTasksTask(cloudtaskspb.Task_FULL)
			testcommon.MustBeIdentical(t, expectedTaskState, taskStateBeforeRun,
				protocmp.Transform(),
				cmpopts.EquateApproxTime(time.Second),
			)

			// wait for the task to be attempted
			require.Eventually(t, func() bool {
				actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
				require.NoError(t, err)
				require.NotNil(t, actualTask)
				return actualTask.FirstAttempt != nil
			}, 300*time.Second, 100*time.Millisecond, "task should eventually be attempted")

			actualTaskAfterRun, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
			require.NoError(t, err)
			require.NotNil(t, actualTaskAfterRun)

			if testCase.shouldPermanentlyFail {

				expectedTaskState := &db.Task{
					Id:               task.Id,
					Name:             task.Name,
					CreatedAt:        task.CreatedAt,
					UpdatedAt:        time.Now(),
					DeletedAt:        testcommon.Ptr(time.Now()),
					QueueID:          task.QueueID,
					LockExpiresAt:    nil,
					MessageType:      task.MessageType,
					HttpRequest:      task.HttpRequest,
					ScheduleTime:     task.ScheduleTime,
					CreateTime:       task.CreateTime,
					DispatchDeadline: task.DispatchDeadline,
					Status:           db.TaskStatusFailed,
					DispatchCount:    task.DispatchCount + 1,
					ResponseCount:    task.ResponseCount + 1,
					FirstAttempt: &db.Task_Attempt{
						ScheduleTime:          task.ScheduleTime,
						DispatchTime:          testcommon.Ptr(time.Now()),
						ResponseTime:          testcommon.Ptr(time.Now()),
						ResponseStatusCode:    int32(testCase.expectedAttemptStatusCode),
						ResponseStatusMessage: "ANY",
					},
					LastAttempt: &db.Task_Attempt{
						ScheduleTime:          task.ScheduleTime,
						DispatchTime:          testcommon.Ptr(time.Now()),
						ResponseTime:          testcommon.Ptr(time.Now()),
						ResponseStatusCode:    int32(testCase.expectedAttemptStatusCode),
						ResponseStatusMessage: "ANY",
					},
				}
				testcommon.MustBeIdentical(t, expectedTaskState, actualTaskAfterRun,
					cmpopts.EquateApproxTime(time.Second),
					cmpopts.IgnoreFields(db.Task_Attempt{}, "ResponseStatusMessage"),
				)
			} else {

				expectedTaskState := &db.Task{
					Id:               task.Id,
					Name:             task.Name,
					CreatedAt:        task.CreatedAt,
					UpdatedAt:        time.Now(),
					DeletedAt:        nil,
					QueueID:          task.QueueID,
					LockExpiresAt:    nil,
					MessageType:      task.MessageType,
					HttpRequest:      task.HttpRequest,
					ScheduleTime:     testcommon.Ptr(calculateNextRetryTime(queue, 1)), // should be in the future now
					CreateTime:       task.CreateTime,
					DispatchDeadline: task.DispatchDeadline,
					Status:           db.TaskStatusPending,
					DispatchCount:    task.DispatchCount + 1,
					ResponseCount:    task.ResponseCount + 1,
					FirstAttempt: &db.Task_Attempt{
						ScheduleTime:          task.ScheduleTime,
						DispatchTime:          testcommon.Ptr(time.Now()),
						ResponseTime:          testcommon.Ptr(time.Now()),
						ResponseStatusCode:    int32(testCase.expectedAttemptStatusCode),
						ResponseStatusMessage: "ANY",
					},
					LastAttempt: &db.Task_Attempt{
						ScheduleTime:          task.ScheduleTime,
						DispatchTime:          testcommon.Ptr(time.Now()),
						ResponseTime:          testcommon.Ptr(time.Now()),
						ResponseStatusCode:    int32(testCase.expectedAttemptStatusCode),
						ResponseStatusMessage: "ANY",
					},
				}
				testcommon.MustBeIdentical(t, expectedTaskState, actualTaskAfterRun,
					cmpopts.EquateApproxTime(time.Second),
					cmpopts.IgnoreFields(db.Task_Attempt{}, "ResponseStatusMessage"),
				)
			}

		}

		testCases := []TestCase{
			// StatusInternalServerError
			{
				testName:                  "Internal Server Error should not permanently fail the task",
				workerResponseStatusCode:  http.StatusInternalServerError,
				shouldPermanentlyFail:     false,
				expectedAttemptStatusCode: codes.Unavailable,
			},
			// StatusTooManyRequests
			{
				testName:                  "Too Many Requests should not permanently fail the task",
				workerResponseStatusCode:  http.StatusTooManyRequests,
				shouldPermanentlyFail:     false,
				expectedAttemptStatusCode: codes.ResourceExhausted,
			},
			// StatusBadGateway
			{
				testName:                  "Bad Gateway should not permanently fail the task",
				workerResponseStatusCode:  http.StatusBadGateway,
				shouldPermanentlyFail:     false,
				expectedAttemptStatusCode: codes.Unavailable,
			},
			// StatusServiceUnavailable
			{
				testName:                  "Service Unavailable should not permanently fail the task",
				workerResponseStatusCode:  http.StatusServiceUnavailable,
				shouldPermanentlyFail:     false,
				expectedAttemptStatusCode: codes.Unavailable,
			},
			// StatusGatewayTimeout
			{
				testName:                  "Gateway Timeout should not permanently fail the task",
				workerResponseStatusCode:  http.StatusGatewayTimeout,
				shouldPermanentlyFail:     false,
				expectedAttemptStatusCode: codes.DeadlineExceeded,
			},
			// all others should fail
			// StatusBadRequest
			{
				testName:                  "Bad Request should permanently fail the task",
				workerResponseStatusCode:  http.StatusBadRequest,
				shouldPermanentlyFail:     true,
				expectedAttemptStatusCode: codes.InvalidArgument,
			},
			// StatusUnauthorized
			{
				testName:                  "Unauthorized should permanently fail the task",
				workerResponseStatusCode:  http.StatusUnauthorized,
				shouldPermanentlyFail:     true,
				expectedAttemptStatusCode: codes.Unauthenticated,
			},
			// StatusForbidden
			{
				testName:                  "Forbidden should permanently fail the task",
				workerResponseStatusCode:  http.StatusForbidden,
				shouldPermanentlyFail:     true,
				expectedAttemptStatusCode: codes.PermissionDenied,
			},
			// StatusNotImplemented
			{
				testName:                  "Not Implemented should permanently fail the task",
				workerResponseStatusCode:  http.StatusNotImplemented,
				shouldPermanentlyFail:     true,
				expectedAttemptStatusCode: codes.Unimplemented,
			},
			// non standard status codes
			{
				testName:                  "Non Standard Status Code should permanently fail the task",
				workerResponseStatusCode:  888,
				shouldPermanentlyFail:     true,
				expectedAttemptStatusCode: codes.Unknown,
			},
		}
		for _, testCase := range testCases {
			t.Run(testCase.testName, func(t *testing.T) {
				t.Helper()
				testFn(t, testCase)
			})
		}
	})
	// - should honor DispatchDeadline:
	//   - create a task with a very short dispatch_deadline and a handler that sleeps longer
	//   - RunTask should result in DeadlineExceeded status from processTask and mark the attempt accordingly
	t.Run("should honor DispatchDeadline", func(t *testing.T) {

		// a server that will sleep longer then the deadline.
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer testServer.Close()

		queue := WhenAQueueIsCreated(t, srv, mongoDB,
			CreateQueueWithRetryConfig(&cloudtaskspb.RetryConfig{MaxAttempts: 1}),
		)
		taskWithShortDeadline := WhenAHttpTargetTaskIsCreated(t, srv, mongoDB, queue, &cloudtaskspb.HttpRequest{
			Url:        testServer.URL,
			HttpMethod: cloudtaskspb.HttpMethod_POST,
			Headers:    nil,
			Body:       nil,
		}, CreateTaskWithDispatchDeadline(10*time.Millisecond))

		_, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name:         taskWithShortDeadline.Name,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, taskWithShortDeadline.Name)
			require.NoError(t, err)
			require.NotNil(t, actualTask)
			return actualTask.FirstAttempt != nil
		}, 10*time.Second, 100*time.Millisecond, "task should eventually be attempted")

		actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, taskWithShortDeadline.Name)
		require.NoError(t, err)
		require.NotNil(t, actualTask)

		expectedTaskState := &db.Task{
			Id:               taskWithShortDeadline.Id,
			Name:             taskWithShortDeadline.Name,
			CreatedAt:        taskWithShortDeadline.CreatedAt,
			UpdatedAt:        time.Now(),
			DeletedAt:        testcommon.Ptr(time.Now()),
			QueueID:          taskWithShortDeadline.QueueID,
			LockExpiresAt:    nil,
			Status:           db.TaskStatusFailed,
			DispatchCount:    taskWithShortDeadline.DispatchCount + 1,
			ResponseCount:    taskWithShortDeadline.ResponseCount + 1,
			MessageType:      taskWithShortDeadline.MessageType,
			HttpRequest:      taskWithShortDeadline.HttpRequest,
			ScheduleTime:     taskWithShortDeadline.ScheduleTime,
			CreateTime:       taskWithShortDeadline.CreateTime,
			DispatchDeadline: taskWithShortDeadline.DispatchDeadline,
			FirstAttempt: &db.Task_Attempt{
				ScheduleTime:          taskWithShortDeadline.ScheduleTime,
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.DeadlineExceeded),
				ResponseStatusMessage: "ANY",
			},
			LastAttempt: &db.Task_Attempt{
				ScheduleTime:          taskWithShortDeadline.ScheduleTime,
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.DeadlineExceeded),
				ResponseStatusMessage: "ANY",
			},
		}
		testcommon.MustBeIdentical(t, expectedTaskState, actualTask,
			cmpopts.EquateApproxTime(time.Second),
			cmpopts.IgnoreFields(db.Task_Attempt{}, "ResponseStatusMessage"),
		)

	})
	// - should respect MaxAttempts and MaxRetryDuration from the queue:
	//   - configure queue retry_config with limited max_attempts and/or max_retry_duration
	//   - simulate repeated RunTask calls with a failing handler and verify:
	//     - after max_attempts or retry duration, task is marked Failed and soft-deleted
	t.Run("should respect MaxAttempts from the queue", func(t *testing.T) {
		oneMinuteDuration := 1 * time.Minute
		queue := WhenAQueueIsCreated(t, srv, mongoDB,
			CreateQueueWithRetryConfig(&cloudtaskspb.RetryConfig{
				MaxAttempts: 2,
				MinBackoff:  durationpb.New(oneMinuteDuration),
				MaxBackoff:  durationpb.New(oneMinuteDuration),
			}),
		)
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer testServer.Close()
		task := WhenAHttpTargetTaskIsCreated(t, srv, mongoDB, queue, &cloudtaskspb.HttpRequest{
			Url:        testServer.URL,
			HttpMethod: cloudtaskspb.HttpMethod_POST,
			Headers:    nil,
			Body:       nil,
		})

		// 1st run
		_, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name:         task.Name,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
			require.NoError(t, err)
			require.NotNil(t, actualTask)
			return actualTask.FirstAttempt != nil
		}, 10*time.Second, 100*time.Millisecond, "task should eventually be attempted")

		actualTaskAfter1stRun, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
		require.NoError(t, err)
		require.NotNil(t, actualTaskAfter1stRun)

		expectedTaskState := &db.Task{
			Id:            task.Id,
			Name:          task.Name,
			CreatedAt:     task.CreatedAt,
			UpdatedAt:     time.Now(),
			DeletedAt:     nil,
			QueueID:       task.QueueID,
			LockExpiresAt: nil,
			Status:        db.TaskStatusPending,
			DispatchCount: task.DispatchCount + 1,
			ResponseCount: task.ResponseCount + 1,
			MessageType:   task.MessageType,
			HttpRequest:   task.HttpRequest,
			// The next schedule timeshould be about oneMinuteDuration from the response time of the first attempt
			ScheduleTime:     testcommon.Ptr(actualTaskAfter1stRun.FirstAttempt.ResponseTime.Add(oneMinuteDuration)),
			CreateTime:       task.CreateTime,
			DispatchDeadline: task.DispatchDeadline,
			FirstAttempt: &db.Task_Attempt{
				ScheduleTime:          task.ScheduleTime,
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.Unavailable),
				ResponseStatusMessage: "ANY",
			},
			LastAttempt: &db.Task_Attempt{
				ScheduleTime:          task.ScheduleTime,
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.Unavailable),
				ResponseStatusMessage: "ANY",
			},
		}
		testcommon.MustBeIdentical(t, expectedTaskState, actualTaskAfter1stRun,
			cmpopts.EquateApproxTime(time.Second),
			cmpopts.IgnoreFields(db.Task_Attempt{}, "ResponseStatusMessage"),
		)

		// 2nd run
		secondRunTime := time.Now()
		_, err = srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name:         task.Name,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
			require.NoError(t, err)
			require.NotNil(t, actualTask)
			return actualTask.DispatchCount == 2
		}, 10*time.Second, 100*time.Millisecond, "task should eventually be attempted")

		actualTaskAfter2ndRun, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
		require.NoError(t, err)
		require.NotNil(t, actualTaskAfter2ndRun)

		expectedTaskStateAfter2ndRun := &db.Task{
			Id:               task.Id,
			Name:             task.Name,
			CreatedAt:        task.CreatedAt,
			UpdatedAt:        time.Now(),
			DeletedAt:        testcommon.Ptr(time.Now()),
			QueueID:          task.QueueID,
			LockExpiresAt:    nil,
			Status:           db.TaskStatusFailed,
			DispatchCount:    2,
			ResponseCount:    2,
			MessageType:      task.MessageType,
			HttpRequest:      task.HttpRequest,
			ScheduleTime:     testcommon.Ptr(secondRunTime), // unchanged since task failed
			CreateTime:       task.CreateTime,
			DispatchDeadline: task.DispatchDeadline,
			FirstAttempt:     actualTaskAfter1stRun.FirstAttempt, // unchanged from the first run
			LastAttempt: &db.Task_Attempt{
				// LastAttempt.ScheduleTime should be the schedule_time when the 2nd RunTask was called
				ScheduleTime:          testcommon.Ptr(secondRunTime),
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.Unavailable),
				ResponseStatusMessage: "ANY",
			},
		}
		testcommon.MustBeIdentical(t, expectedTaskStateAfter2ndRun, actualTaskAfter2ndRun,
			cmpopts.EquateApproxTime(time.Second),
			cmpopts.IgnoreFields(db.Task_Attempt{}, "ResponseStatusMessage"),
		)
	})

	t.Run("should respect MaxRetryDuration from the queue", func(t *testing.T) {
		oneMinuteDuration := 1 * time.Minute
		maxRetryDuration := 100 * time.Millisecond
		queue := WhenAQueueIsCreated(t, srv, mongoDB,
			CreateQueueWithRetryConfig(&cloudtaskspb.RetryConfig{
				MaxAttempts:      2,
				MinBackoff:       durationpb.New(oneMinuteDuration),
				MaxBackoff:       durationpb.New(oneMinuteDuration),
				MaxRetryDuration: durationpb.New(maxRetryDuration),
			}),
		)
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// our request will hold up the worker past the max retry duration.
			time.Sleep(maxRetryDuration + 5*time.Millisecond)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer testServer.Close()
		task := WhenAHttpTargetTaskIsCreated(t, srv, mongoDB, queue, &cloudtaskspb.HttpRequest{
			Url:        testServer.URL,
			HttpMethod: cloudtaskspb.HttpMethod_POST,
			Headers:    nil,
			Body:       nil,
		})

		// 1st run
		_, err := srv.RunTask(ctx, &cloudtaskspb.RunTaskRequest{
			Name:         task.Name,
			ResponseView: cloudtaskspb.Task_FULL,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			actualTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
			require.NoError(t, err)
			require.NotNil(t, actualTask)
			return actualTask.FirstAttempt != nil
		}, 10*time.Second, 100*time.Millisecond, "task should eventually be attempted")

		actualTaskAfter1stRun, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, task.Name)
		require.NoError(t, err)
		require.NotNil(t, actualTaskAfter1stRun)

		expectedTaskState := &db.Task{
			Id:               task.Id,
			Name:             task.Name,
			CreatedAt:        task.CreatedAt,
			UpdatedAt:        time.Now(),
			DeletedAt:        testcommon.Ptr(time.Now()),
			QueueID:          task.QueueID,
			LockExpiresAt:    nil,
			Status:           db.TaskStatusFailed,
			DispatchCount:    task.DispatchCount + 1,
			ResponseCount:    task.ResponseCount + 1,
			MessageType:      task.MessageType,
			HttpRequest:      task.HttpRequest,
			ScheduleTime:     task.ScheduleTime, // unchanged since task was not retried
			CreateTime:       task.CreateTime,
			DispatchDeadline: task.DispatchDeadline,
			FirstAttempt: &db.Task_Attempt{
				ScheduleTime:          task.ScheduleTime,
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.Unavailable),
				ResponseStatusMessage: "ANY",
			},
			LastAttempt: &db.Task_Attempt{
				ScheduleTime:          task.ScheduleTime,
				DispatchTime:          testcommon.Ptr(time.Now()),
				ResponseTime:          testcommon.Ptr(time.Now()),
				ResponseStatusCode:    int32(codes.Unavailable),
				ResponseStatusMessage: "ANY",
			},
		}
		testcommon.MustBeIdentical(t, expectedTaskState, actualTaskAfter1stRun,
			cmpopts.EquateApproxTime(time.Second),
			cmpopts.IgnoreFields(db.Task_Attempt{}, "ResponseStatusMessage"),
		)

	})
}

type CreateQueueOptions struct {
	RetryConfig *cloudtaskspb.RetryConfig
}
type CreateQueueOptionsFn func(options *CreateQueueOptions)

func CreateQueueWithUnlimitedRetries() CreateQueueOptionsFn {
	return CreateQueueWithRetryConfig(&cloudtaskspb.RetryConfig{
		MaxAttempts: -1,
	})
}

func CreateQueueWithRetryConfig(retryConfig *cloudtaskspb.RetryConfig) CreateQueueOptionsFn {
	return func(options *CreateQueueOptions) {
		options.RetryConfig = retryConfig
	}
}

func WhenAQueueIsCreated(t *testing.T, srv *Server, mongoDB *mongo.Database, opts ...CreateQueueOptionsFn) *db.Queue {
	t.Helper()

	options := &CreateQueueOptions{
		RetryConfig: nil,
	}
	for _, opt := range opts {
		opt(options)
	}

	ctx := context.Background()
	// - create a queue
	queueCustomName := primitive.NewObjectID().Hex()
	queueName := "projects/test-project/locations/us-central1/queues/" + queueCustomName
	createdCloudQueue, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/test-project/locations/us-central1",
		Queue: &cloudtaskspb.Queue{
			Name:        queueName,
			RetryConfig: options.RetryConfig,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createdCloudQueue)

	dbCreatedQueue, err := db.SelectQueueByName(ctx, mongoDB, createdCloudQueue.Name)
	require.NoError(t, err)
	require.NotNil(t, dbCreatedQueue)

	return dbCreatedQueue
}

type CreateTaskOptionsFn func(options *cloudtaskspb.Task)

func CreateTaskWithDispatchDeadline(dispatchDeadline time.Duration) CreateTaskOptionsFn {
	return func(task *cloudtaskspb.Task) {
		task.DispatchDeadline = durationpb.New(dispatchDeadline)
	}
}

func WhenAHttpTargetTaskIsCreated(t *testing.T, srv *Server, mongoDB *mongo.Database, queue *db.Queue, req *cloudtaskspb.HttpRequest, opts ...CreateTaskOptionsFn) *db.Task {
	t.Helper()
	ctx := context.Background()
	// - create a task
	taskName := queue.Name + "/tasks/" + primitive.NewObjectID().Hex()

	task := &cloudtaskspb.Task{
		Name: taskName,
		MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: req,
		},
	}
	for _, opt := range opts {
		opt(task)
	}
	createdCloudTask, err := srv.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{
		Parent: queue.Name,
		Task:   task,
	})
	require.NoError(t, err)
	require.NotNil(t, createdCloudTask)

	dbCreatedTask, err := db.SelectTaskByNameIncludingDeleted(ctx, mongoDB, createdCloudTask.Name)
	require.NoError(t, err)
	require.NotNil(t, dbCreatedTask)

	return dbCreatedTask
}
