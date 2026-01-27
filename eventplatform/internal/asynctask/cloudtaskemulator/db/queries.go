package db

import (
	"context"
	"fmt"
	"time"

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
