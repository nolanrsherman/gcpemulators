package db

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const testMongoURI = "mongodb://localhost:27017/?directConnection=true"

func TestRunMigrations(t *testing.T) {
	ctx := context.Background()
	db, cleanup := NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := RunMigrations(testMongoURI, db.Name())
	require.NoError(t, err)
}

func TestInsertQueue(t *testing.T) {
	ctx := context.Background()
	db, cleanup := NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	RunMigrations(testMongoURI, db.Name())

	t.Run("should be able to insert a queue", func(t *testing.T) {
		queue := &Queue{
			Name:      "projects/test-project/locations/us-central1/queues/test-queue",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		result, err := InsertQueue(ctx, db, queue)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotEmpty(t, result.Id)

		// Verify the queue was inserted
		var found Queue
		err = db.Collection(CollectionQueues).FindOne(ctx, bson.M{"_id": result.Id}).Decode(&found)
		require.NoError(t, err)
		require.Equal(t, queue.Name, found.Name)
		require.NotEmpty(t, found.Id)
	})

	t.Run("should fail if a queue with the same name already exists", func(t *testing.T) {
		queueName := "projects/test-project/locations/us-central1/queues/duplicate-queue"

		// Insert first queue
		queue1 := &Queue{
			Name:      queueName,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		_, err := InsertQueue(ctx, db, queue1)
		require.NoError(t, err)

		// Try to insert a second queue with the same name
		queue2 := &Queue{
			Name:      queueName,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		_, err = InsertQueue(ctx, db, queue2)
		require.Error(t, err)
		require.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate key error, got: %v", err)
	})
}

func TestInsertTask(t *testing.T) {
	ctx := context.Background()
	db, cleanup := NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	RunMigrations(testMongoURI, db.Name())

	t.Run("should be able to insert a task", func(t *testing.T) {
		task := &Task{
			Name:        "projects/test-project/locations/us-central1/queues/test-queue/tasks/test-task-123",
			MessageType: "Task_HttpRequest",
			HttpRequest: &Task_HttpRequest{
				HttpMethod: cloudtaskspb.HttpMethod_POST,
				Url:        "https://example.com/api/endpoint",
				Headers:    map[string]string{"Content-Type": "application/json"},
				Body:       []byte(`{"ok":true}`),
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		result, err := InsertTask(ctx, db, task)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.NotEmpty(t, result.Id)

		// Verify the task was inserted
		var found Task
		err = db.Collection(CollectionTasks).FindOne(ctx, bson.M{"_id": result.Id}).Decode(&found)
		require.NoError(t, err)
		require.Equal(t, task.Name, found.Name)
		require.Equal(t, task.MessageType, found.MessageType)
		require.NotNil(t, found.HttpRequest)
		require.Equal(t, task.HttpRequest.Url, found.HttpRequest.Url)
		require.NotEmpty(t, found.Id)
	})

	t.Run("should fail if a task with the same name already exists", func(t *testing.T) {
		taskName := "projects/test-project/locations/us-central1/queues/test-queue/tasks/duplicate-task-456"

		// Insert first task
		task1 := &Task{
			Name:        taskName,
			MessageType: "Task_HttpRequest",
			HttpRequest: &Task_HttpRequest{
				HttpMethod: cloudtaskspb.HttpMethod_POST,
				Url:        "https://example.com/api/endpoint1",
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		_, err := InsertTask(ctx, db, task1)
		require.NoError(t, err)

		// Try to insert a second task with the same name
		task2 := &Task{
			Name:        taskName,
			MessageType: "Task_HttpRequest",
			HttpRequest: &Task_HttpRequest{
				HttpMethod: cloudtaskspb.HttpMethod_GET,
				Url:        "https://example.com/api/endpoint2",
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		_, err = InsertTask(ctx, db, task2)
		require.Error(t, err)
		require.True(t, mongo.IsDuplicateKeyError(err), "expected duplicate key error, got: %v", err)
	})
}
