package db

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Unique Index on queue.name
const CollectionQueues = "cloudtaskemulator_queues"

// Unique Index on task.name
// Index on task.schedule_time
const CollectionTasks = "cloudtaskemulator_tasks"

func SelectQueueByName(ctx context.Context, db *mongo.Database, name string) (*Queue, error) {
	col := db.Collection(CollectionQueues)
	var queue Queue
	// Only return queues that have not been soft-deleted.
	err := col.FindOne(ctx, bson.M{
		"name":       name,
		"deleted_at": bson.M{"$exists": false},
	}).Decode(&queue)
	if err != nil {
		return nil, fmt.Errorf("failed to select queue by name: %w", err)
	}
	return &queue, nil
}

// SoftDeleteQueueByName marks a queue as deleted by setting DeletedAt. A TTL
// index on this field is responsible for physically removing the document
// after a retention period.
func SoftDeleteQueueByName(ctx context.Context, db *mongo.Database, name string) error {
	col := db.Collection(CollectionQueues)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{
			"name":       name,
			"deleted_at": bson.M{"$exists": false},
		},
		bson.M{
			"$set": bson.M{
				"deleted_at": now,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to soft delete queue: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to soft delete queue: %w", mongo.ErrNoDocuments)
	}
	return nil
}

func InsertQueue(ctx context.Context, db *mongo.Database, queue *Queue) (*Queue, error) {
	col := db.Collection(CollectionQueues)
	result, err := col.InsertOne(ctx, queue)
	if err != nil {
		return nil, fmt.Errorf("failed to insert queue: %w", err)
	}
	oid, ok := result.InsertedID.(primitive.ObjectID)
	if !ok {
		return nil, fmt.Errorf("failed to convert inserted ID to ObjectID: %v", result.InsertedID)
	}
	queue.Id = oid
	return queue, nil
}

func InsertTask(ctx context.Context, db *mongo.Database, task *Task) (*Task, error) {
	col := db.Collection(CollectionTasks)
	result, err := col.InsertOne(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to insert task: %w", err)
	}
	oid, ok := result.InsertedID.(primitive.ObjectID)
	if !ok {
		return nil, fmt.Errorf("failed to convert inserted ID to ObjectID: %v", result.InsertedID)
	}
	task.Id = oid
	return task, nil
}

// UpdateQueueState updates the state of a queue by name. Only updates queues
// that have not been soft-deleted.
func UpdateQueueState(ctx context.Context, db *mongo.Database, name string, state cloudtaskspb.Queue_State) error {
	col := db.Collection(CollectionQueues)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{
			"name":       name,
			"deleted_at": bson.M{"$exists": false},
		},
		bson.M{
			"$set": bson.M{
				"state":      state,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update queue state: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to update queue state: %w", mongo.ErrNoDocuments)
	}
	return nil
}

func SelectQueuesWithIDNotInList(ctx context.Context, db *mongo.Database, ids []primitive.ObjectID) ([]Queue, error) {
	col := db.Collection(CollectionQueues)
	cursor, err := col.Find(ctx, bson.M{
		"_id": bson.M{"$nin": ids},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to select queues with ID not in list: %w", err)
	}
	queues := make([]Queue, 0)
	err = cursor.All(ctx, &queues)
	if err != nil {
		return nil, fmt.Errorf("failed to decode queues: %w", err)
	}
	return queues, nil
}

func SelectAllQueuesWithDeletedAtNotSet(ctx context.Context, db *mongo.Database) ([]Queue, error) {
	col := db.Collection(CollectionQueues)
	cursor, err := col.Find(ctx, bson.M{
		"deleted_at": bson.M{"$exists": false},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to select all queues: %w", err)
	}
	queues := make([]Queue, 0)
	err = cursor.All(ctx, &queues)
	if err != nil {
		return nil, fmt.Errorf("failed to decode queues: %w", err)
	}
	return queues, nil
}
