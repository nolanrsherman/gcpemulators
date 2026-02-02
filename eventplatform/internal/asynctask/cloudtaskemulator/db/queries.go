package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

func SelectQueueByID(ctx context.Context, db *mongo.Database, id primitive.ObjectID) (*Queue, error) {
	col := db.Collection(CollectionQueues)
	var queue Queue
	// Only return queues that have not been soft-deleted.
	err := col.FindOne(ctx, bson.M{
		"_id":        id,
		"deleted_at": bson.M{"$exists": false},
	}).Decode(&queue)
	if err != nil {
		return nil, fmt.Errorf("failed to select queue by ID: %w", err)
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

func SelectQueuesWithIDNotInList(ctx context.Context, db *mongo.Database, ids []primitive.ObjectID) ([]*Queue, error) {
	col := db.Collection(CollectionQueues)
	cursor, err := col.Find(ctx, bson.M{
		"_id": bson.M{"$nin": ids},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to select queues with ID not in list: %w", err)
	}
	queues := make([]*Queue, 0)
	err = cursor.All(ctx, &queues)
	if err != nil {
		return nil, fmt.Errorf("failed to decode queues: %w", err)
	}
	return queues, nil
}

func SelectAllQueuesWithDeletedAtNotSet(ctx context.Context, db *mongo.Database) ([]*Queue, error) {
	col := db.Collection(CollectionQueues)
	cursor, err := col.Find(ctx, bson.M{
		"deleted_at": bson.M{"$exists": false},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to select all queues: %w", err)
	}
	queues := make([]*Queue, 0)
	err = cursor.All(ctx, &queues)
	if err != nil {
		return nil, fmt.Errorf("failed to decode queues: %w", err)
	}
	return queues, nil
}

func SelectTaskByName(ctx context.Context, db *mongo.Database, name string) (*Task, error) {
	col := db.Collection(CollectionTasks)
	var task Task
	err := col.FindOne(ctx, bson.M{
		"name":       name,
		"deleted_at": bson.M{"$exists": false},
	}).Decode(&task)
	if err != nil {
		return nil, fmt.Errorf("failed to select task by name: %w", err)
	}
	return &task, nil
}

// SoftDeleteTaskByName marks a task as deleted by setting DeletedAt. A TTL
// index on this field is responsible for physically removing the document
// after a retention period.
func SoftDeleteTaskByName(ctx context.Context, db *mongo.Database, name string) error {
	col := db.Collection(CollectionTasks)
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
		return fmt.Errorf("failed to soft delete task: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to soft delete task: %w", mongo.ErrNoDocuments)
	}
	return nil
}

// SelectNextTaskAndLock atomically claims the next available task
// for the given queue and marks it as running. It also sets a lock expiration
// based on the task's dispatch_deadline to allow stuck tasks to be reclaimed:
//
//	lock_expires_at = now + dispatch_deadline + 30s
//
// If dispatch_deadline is not set, a reasonable default (10 minutes) is used.
func SelectNextTaskAndLock(ctx context.Context, db *mongo.Database, queueId primitive.ObjectID) (*Task, error) {
	col := db.Collection(CollectionTasks)
	now := time.Now()

	// Query for tasks that are:
	// 1. In the correct queue
	// 2. Either pending (never claimed) OR running with expired lock (stuck)
	// 3. Scheduled to run now or earlier
	query := bson.M{
		"queue_id":   queueId,
		"deleted_at": bson.M{"$exists": false},
		"$or": []bson.M{
			{
				"status":        TaskStatusPending,
				"schedule_time": bson.M{"$lte": now},
			},
			{
				"status":          TaskStatusRunning,
				"schedule_time":   bson.M{"$lte": now},
				"lock_expires_at": bson.M{"$lt": now}, // Lock expired (stuck task)
			},
		},
	}

	// First, atomically claim the task and set a temporary lock_expires_at
	// slightly in the future so no other worker can match it in the same query.
	// We will refine lock_expires_at based on dispatch_deadline after we know
	// which task we claimed.
	tempLease := 1 * time.Minute
	update := bson.M{
		"$set": bson.M{
			"status":          TaskStatusRunning,
			"lock_expires_at": now.Add(tempLease),
			"updated_at":      now,
		},
	}

	// Sort: Get earliest scheduled (matches index)
	sort := bson.D{{Key: "schedule_time", Value: 1}}

	opts := options.FindOneAndUpdate().
		SetSort(sort).
		SetReturnDocument(options.After) // Return the updated document

	var task Task
	err := col.FindOneAndUpdate(ctx, query, update, opts).Decode(&task)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// No task available - this is normal, not an error
			return nil, nil
		}
		return nil, err
	}

	// Compute lock_expires_at from task.DispatchDeadline: now + deadline + 30s.
	// If no dispatch_deadline is set, use 10 minutes as a default HTTP deadline.
	leaseDuration := 10 * time.Minute
	if task.DispatchDeadline != nil {
		leaseDuration = *task.DispatchDeadline
	}
	lockExpiresAt := now.Add(leaseDuration + 30*time.Second)

	_, err = col.UpdateOne(ctx, bson.M{"_id": task.Id}, bson.M{
		"$set": bson.M{
			"lock_expires_at": lockExpiresAt,
		},
	})
	if err != nil {
		return nil, err
	}

	return &task, nil
}

func SelectTaskByIDIncludingDeleted(ctx context.Context, db *mongo.Database, id primitive.ObjectID) (*Task, error) {
	col := db.Collection(CollectionTasks)
	var task Task
	err := col.FindOne(ctx, bson.M{"_id": id}).Decode(&task)
	if err != nil {
		return nil, fmt.Errorf("failed to select task by ID: %w", err)
	}
	return &task, nil
}

func SelectTaskByNameIncludingDeleted(ctx context.Context, db *mongo.Database, name string) (*Task, error) {
	col := db.Collection(CollectionTasks)
	var task Task
	err := col.FindOne(ctx, bson.M{"name": name}).Decode(&task)
	if err != nil {
		return nil, fmt.Errorf("failed to select task by name: %w", err)
	}
	return &task, nil
}
