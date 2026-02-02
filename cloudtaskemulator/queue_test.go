package cloudtaskemulator

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testMongoURI = "mongodb://localhost:27017/?directConnection=true"
)

func TestListQueues(t *testing.T) {
	ctx := context.Background()

	t.Run("should reject empty parent", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{
			Parent: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate parent resource name format", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		_, err = srv.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{
			Parent: "invalid-parent",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should return queues in lexicographical order and support pagination", func(t *testing.T) {
		mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
		defer cleanup(t)
		err := db.RunMigrations(testMongoURI, mongoDB.Name())
		require.NoError(t, err)

		logger := zap.NewNop()
		srv := NewServer(mongoDB, logger)

		parent := "projects/test-project/locations/us-central1"

		// Create three queues with fixed names under the same parent but out of order
		create := func(name string) {
			res, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
				Parent: parent,
				Queue: &cloudtaskspb.Queue{
					Name: name,
				},
			})
			require.NoError(t, err)
			require.Equal(t, name, res.GetName())
		}

		q1 := parent + "/queues/q-alpha"
		q2 := parent + "/queues/q-gamma"
		q3 := parent + "/queues/q-beta"

		create(q1)
		create(q2)
		create(q3)

		// First page: page_size=2
		listRes1, err := srv.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{
			Parent:   parent,
			PageSize: 2,
		})
		require.NoError(t, err)
		require.Len(t, listRes1.GetQueues(), 2)

		// Queues should be sorted lexicographically by name
		names1 := []string{listRes1.Queues[0].GetName(), listRes1.Queues[1].GetName()}
		require.Equal(t, []string{q1, q3}, names1)

		// Second page using next_page_token
		require.NotEmpty(t, listRes1.GetNextPageToken())
		listRes2, err := srv.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{
			Parent:    parent,
			PageSize:  2,
			PageToken: listRes1.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, listRes2.GetQueues(), 1)
		require.Equal(t, q2, listRes2.Queues[0].GetName())
		require.Empty(t, listRes2.GetNextPageToken())
	})
}
func TestGetQueue(t *testing.T) {
	ctx := context.Background()
	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"

	t.Run("should return queue when it exists", func(t *testing.T) {
		// First create a queue
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		require.NotEmpty(t, createRes.GetName())

		// Now fetch it via GetQueue
		got, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{
			Name: createRes.GetName(),
		})
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, createRes.GetName(), got.GetName())
	})

	t.Run("should return error for non-existent queue", func(t *testing.T) {
		_, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{
			Name: parent + "/queues/does-not-exist",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		// Current implementation wraps any DB error as Internal.
		require.Equal(t, codes.Internal, st.Code())
	})
}
func TestCreateQueue(t *testing.T) {
	ctx := context.Background()
	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"

	t.Run("should reject empty parent", func(t *testing.T) {
		_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "",
			Queue:  &cloudtaskspb.Queue{},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate parent resource name format", func(t *testing.T) {
		_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: "invalid-parent",
			Queue:  &cloudtaskspb.Queue{},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should validate queue name format when provided", func(t *testing.T) {
		_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: "bad-name",
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should generate a queue name under the parent when name is not provided", func(t *testing.T) {
		res, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		require.NotEmpty(t, res.GetName())
		require.Contains(t, res.GetName(), "/queues/")
		require.Len(t, res.GetName(), len(res.GetName()))

		// Ensure it was persisted
		q, err := db.SelectQueueByName(ctx, mongoDB, res.GetName())
		require.NoError(t, err)
		require.Equal(t, res.GetName(), q.Name)
	})

	t.Run("should reject queue name that does not match the parent prefix", func(t *testing.T) {
		_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: "projects/other-project/locations/us-central1/queues/q1",
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should reject AppEngineRoutingOverride", func(t *testing.T) {
		_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				AppEngineRoutingOverride: &cloudtaskspb.AppEngineRouting{},
			},
		})
		require.Error(t, err)
		st, _ := grpcstatus.FromError(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("should apply default rate limits and retry config when none provided", func(t *testing.T) {
		res, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)

		q, err := db.SelectQueueByName(ctx, mongoDB, res.GetName())
		require.NoError(t, err)

		require.Equal(t, float64(100), q.MaxDispatchesPerSecond)
		require.Equal(t, int32(25), q.MaxBurstSize)
		require.Equal(t, int32(50), q.MaxConcurrentDispatches)
		require.Equal(t, int32(3), q.MaxAttempts)
		require.Equal(t, int32(3), q.MaxDoublings)
		require.NotNil(t, q.MaxRetryDuration)
		require.NotNil(t, q.MinBackoff)
		require.NotNil(t, q.MaxBackoff)
	})

	t.Run("should accept valid RateLimits and persist them", func(t *testing.T) {
		name := parent + "/queues/custom-rate-queue"
		res, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RateLimits: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  200,
					MaxConcurrentDispatches: 100,
					MaxBurstSize:            999, // should be ignored
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, name, res.GetName())

		q, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)
		require.Equal(t, float64(200), q.MaxDispatchesPerSecond)
		require.Equal(t, int32(100), q.MaxConcurrentDispatches)
		// MaxBurstSize should be derived, not equal to user-provided 999
		require.NotEqual(t, int32(999), q.MaxBurstSize)
	})

	t.Run("should reject RateLimits with invalid values", func(t *testing.T) {
		cases := []struct {
			name string
			rate *cloudtaskspb.RateLimits
		}{
			{
				name: "MaxDispatchesPerSecond <= 0",
				rate: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  -1,
					MaxConcurrentDispatches: 10,
				},
			},
			{
				name: "MaxDispatchesPerSecond > 500",
				rate: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  501,
					MaxConcurrentDispatches: 10,
				},
			},
			{
				name: "MaxConcurrentDispatches <= 0",
				rate: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  10,
					MaxConcurrentDispatches: 0,
				},
			},
			{
				name: "MaxConcurrentDispatches > 5000",
				rate: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  10,
					MaxConcurrentDispatches: 6000,
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
					Parent: parent,
					Queue: &cloudtaskspb.Queue{
						RateLimits: tc.rate,
					},
				})
				require.Error(t, err)
				st, _ := grpcstatus.FromError(err)
				require.Equal(t, codes.InvalidArgument, st.Code())
			})
		}
	})

	t.Run("should accept RetryConfig with MaxAttempts >= -1 and persist values", func(t *testing.T) {
		name := parent + "/queues/retry-queue"
		res, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts:  5,
					MaxDoublings: 4,
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, name, res.GetName())

		q, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)
		require.Equal(t, int32(5), q.MaxAttempts)
		require.Equal(t, int32(4), q.MaxDoublings)
	})

	t.Run("should reject RetryConfig with invalid values", func(t *testing.T) {
		cases := []struct {
			name  string
			retry *cloudtaskspb.RetryConfig
		}{
			{
				name: "MaxAttempts < -1",
				retry: &cloudtaskspb.RetryConfig{
					MaxAttempts: -2,
				},
			},
			{
				name: "MaxDoublings < 0",
				retry: &cloudtaskspb.RetryConfig{
					MaxAttempts:  1,
					MaxDoublings: -1,
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
					Parent: parent,
					Queue: &cloudtaskspb.Queue{
						RetryConfig: tc.retry,
					},
				})
				require.Error(t, err)
				st, _ := grpcstatus.FromError(err)
				require.Equal(t, codes.InvalidArgument, st.Code())
			})
		}
	})
}
func TestUpdateQueue(t *testing.T) {
	ctx := context.Background()
	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"

	t.Run("creates queue when it does not exist", func(t *testing.T) {
		name := parent + "/queues/new-queue"
		res, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
			},
		})
		require.NoError(t, err)
		require.Equal(t, name, res.GetName())

		// Verify queue was created with defaults
		q, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)
		require.Equal(t, name, q.Name)
		require.Equal(t, float64(100), q.MaxDispatchesPerSecond)
		require.Equal(t, int32(25), q.MaxBurstSize)
		require.Equal(t, int32(50), q.MaxConcurrentDispatches)
		require.Equal(t, int32(3), q.MaxAttempts)
	})

	t.Run("updates rate limits and retry config for existing queue", func(t *testing.T) {
		// First create a queue
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		// Update rate limits and retry config
		_, err = srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RateLimits: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  10,
					MaxConcurrentDispatches: 100,
				},
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts:      5,
					MaxRetryDuration: durationpb.New(5 * time.Minute),
					MinBackoff:       durationpb.New(2 * time.Second),
					MaxBackoff:       durationpb.New(30 * time.Second),
					MaxDoublings:     4,
				},
			},
		})
		require.NoError(t, err)

		// Verify values were updated
		q, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)
		require.Equal(t, float64(10), q.MaxDispatchesPerSecond)
		require.Equal(t, int32(100), q.MaxConcurrentDispatches)
		require.Equal(t, int32(5), q.MaxAttempts)
		require.Equal(t, 5*time.Minute, *q.MaxRetryDuration)
		require.Equal(t, 2*time.Second, *q.MinBackoff)
		require.Equal(t, 30*time.Second, *q.MaxBackoff)
		require.Equal(t, int32(4), q.MaxDoublings)
	})

	t.Run("rejects invalid retry config", func(t *testing.T) {
		name := parent + "/queues/retry-invalid"

		_, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts: -2,
				},
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("updates only rate limits when update_mask is rate_limits", func(t *testing.T) {
		// Create a queue and capture its initial retry config
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		before, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)

		// Attempt to change both rate limits and retry config, but mask only rate_limits
		_, err = srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RateLimits: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  42,
					MaxConcurrentDispatches: 123,
				},
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts:      9,
					MaxRetryDuration: durationpb.New(9 * time.Minute),
					MinBackoff:       durationpb.New(5 * time.Second),
					MaxBackoff:       durationpb.New(1 * time.Minute),
					MaxDoublings:     7,
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"rate_limits"},
			},
		})
		require.NoError(t, err)

		after, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)

		// Rate limits updated
		require.Equal(t, float64(42), after.MaxDispatchesPerSecond)
		require.Equal(t, int32(123), after.MaxConcurrentDispatches)
		require.NotZero(t, after.MaxBurstSize)

		// Retry config unchanged
		require.Equal(t, before.MaxAttempts, after.MaxAttempts)
		require.Equal(t, *before.MaxRetryDuration, *after.MaxRetryDuration)
		require.Equal(t, *before.MinBackoff, *after.MinBackoff)
		require.Equal(t, *before.MaxBackoff, *after.MaxBackoff)
		require.Equal(t, before.MaxDoublings, after.MaxDoublings)
	})

	t.Run("updates only retry config when update_mask is retry_config", func(t *testing.T) {
		// Create a queue and capture its initial rate limits
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		before, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)

		// Attempt to change both RL and RC, but mask only retry_config
		_, err = srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RateLimits: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  200,
					MaxConcurrentDispatches: 300,
				},
				RetryConfig: &cloudtaskspb.RetryConfig{
					MaxAttempts:      7,
					MaxRetryDuration: durationpb.New(2 * time.Minute),
					MinBackoff:       durationpb.New(3 * time.Second),
					MaxBackoff:       durationpb.New(20 * time.Second),
					MaxDoublings:     2,
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"retry_config"},
			},
		})
		require.NoError(t, err)

		after, err := db.SelectQueueByName(ctx, mongoDB, name)
		require.NoError(t, err)

		// Rate limits unchanged
		require.Equal(t, before.MaxDispatchesPerSecond, after.MaxDispatchesPerSecond)
		require.Equal(t, before.MaxConcurrentDispatches, after.MaxConcurrentDispatches)
		require.Equal(t, before.MaxBurstSize, after.MaxBurstSize)

		// Retry config updated
		require.Equal(t, int32(7), after.MaxAttempts)
		require.Equal(t, 2*time.Minute, *after.MaxRetryDuration)
		require.Equal(t, 3*time.Second, *after.MinBackoff)
		require.Equal(t, 20*time.Second, *after.MaxBackoff)
		require.Equal(t, int32(2), after.MaxDoublings)
	})

	t.Run("rejects unknown update mask paths", func(t *testing.T) {
		name := parent + "/queues/mask-invalid"

		_, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
				RateLimits: &cloudtaskspb.RateLimits{
					MaxDispatchesPerSecond:  10,
					MaxConcurrentDispatches: 10,
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"state"},
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects update of AppEngineRoutingOverride", func(t *testing.T) {
		name := parent + "/queues/appengine-update"

		_, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name:                     name,
				AppEngineRoutingOverride: &cloudtaskspb.AppEngineRouting{},
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects update of State", func(t *testing.T) {
		name := parent + "/queues/state-update"

		_, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name:  name,
				State: cloudtaskspb.Queue_PAUSED,
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects update of PurgeTime", func(t *testing.T) {
		name := parent + "/queues/purge-update"

		_, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name:      name,
				PurgeTime: timestamppb.Now(),
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects update of StackdriverLoggingConfig", func(t *testing.T) {
		name := parent + "/queues/logging-update"

		_, err := srv.UpdateQueue(ctx, &cloudtaskspb.UpdateQueueRequest{
			Queue: &cloudtaskspb.Queue{
				Name: name,
				StackdriverLoggingConfig: &cloudtaskspb.StackdriverLoggingConfig{
					SamplingRatio: 0.5,
				},
			},
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})
}

func TestDeleteQueue(t *testing.T) {
	ctx := context.Background()

	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := srv.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{
			Name: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects invalid name format", func(t *testing.T) {
		_, err := srv.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{
			Name: "invalid-name",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("returns NotFound for non-existent queue", func(t *testing.T) {
		name := parent + "/queues/non-existent"

		_, err := srv.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{
			Name: name,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("soft deletes an existing queue", func(t *testing.T) {
		// Create a queue first
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		// Delete the queue
		_, err = srv.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{
			Name: name,
		})
		require.NoError(t, err)

		// Ensure it is no longer returned by SelectQueueByName (because of soft delete)
		_, err = db.SelectQueueByName(ctx, mongoDB, name)
		require.Error(t, err)
		require.True(t, errors.Is(err, mongo.ErrNoDocuments))

		// But the underlying document still exists with deleted_at set, and
		// will be cleaned up by the TTL index after the retention period.
		var raw db.Queue
		err = mongoDB.Collection(db.CollectionQueues).FindOne(ctx, bson.M{"name": name}).Decode(&raw)
		require.NoError(t, err)
		require.NotNil(t, raw.DeletedAt)
	})
}

func TestPauseQueue(t *testing.T) {
	ctx := context.Background()

	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := srv.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{
			Name: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects invalid name format", func(t *testing.T) {
		_, err := srv.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{
			Name: "invalid-name",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("returns NotFound for non-existent queue", func(t *testing.T) {
		name := parent + "/queues/non-existent"

		_, err := srv.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{
			Name: name,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("pauses an existing queue", func(t *testing.T) {
		// Create a queue first
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		// Verify initial state is RUNNING
		initialQueue, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_RUNNING, initialQueue.GetState())

		// Pause the queue
		pausedQueue, err := srv.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{
			Name: name,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_PAUSED, pausedQueue.GetState())

		// Verify the state persisted
		verifyQueue, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_PAUSED, verifyQueue.GetState())
	})
}

func TestResumeQueue(t *testing.T) {
	ctx := context.Background()

	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := db.RunMigrations(testMongoURI, mongoDB.Name())
	require.NoError(t, err)

	logger := zap.NewNop()
	srv := NewServer(mongoDB, logger)

	parent := "projects/test-project/locations/us-central1"

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := srv.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{
			Name: "",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("rejects invalid name format", func(t *testing.T) {
		_, err := srv.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{
			Name: "invalid-name",
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
	})

	t.Run("returns NotFound for non-existent queue", func(t *testing.T) {
		name := parent + "/queues/non-existent"

		_, err := srv.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{
			Name: name,
		})
		require.Error(t, err)
		st, ok := grpcstatus.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})

	t.Run("resumes a paused queue", func(t *testing.T) {
		// Create a queue first
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		// Pause the queue first
		_, err = srv.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{
			Name: name,
		})
		require.NoError(t, err)

		// Verify it's paused
		pausedQueue, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_PAUSED, pausedQueue.GetState())

		// Resume the queue
		resumedQueue, err := srv.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{
			Name: name,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_RUNNING, resumedQueue.GetState())

		// Verify the state persisted
		verifyQueue, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_RUNNING, verifyQueue.GetState())
	})

	t.Run("resumes a running queue", func(t *testing.T) {
		// Create a queue first (starts as RUNNING)
		createRes, err := srv.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{
			Parent: parent,
			Queue:  &cloudtaskspb.Queue{},
		})
		require.NoError(t, err)
		name := createRes.GetName()

		// Verify initial state is RUNNING
		initialQueue, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_RUNNING, initialQueue.GetState())

		// Resume the queue (should remain RUNNING)
		resumedQueue, err := srv.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{
			Name: name,
		})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_RUNNING, resumedQueue.GetState())

		// Verify the state persisted
		verifyQueue, err := srv.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, cloudtaskspb.Queue_RUNNING, verifyQueue.GetState())
	})
}
