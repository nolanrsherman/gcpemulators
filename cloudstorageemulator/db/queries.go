package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/genproto/googleapis/storage/v2"
)

const (
	CollectionBuckets = "cloudstorageemulator_buckets"
)

func InsertBucket(ctx context.Context, db *mongo.Database, bucket *storage.Bucket) (*storage.Bucket, error) {
	col := db.Collection(CollectionBuckets)
	now := time.Now()

	doc := &BucketDocument{
		CreatedAt: now,
		UpdatedAt: now,
		Bucket:    bucket,
	}

	result, err := col.InsertOne(ctx, doc)
	if err != nil {
		return nil, fmt.Errorf("failed to insert bucket: %w", err)
	}

	oid, ok := result.InsertedID.(primitive.ObjectID)
	if !ok {
		return nil, fmt.Errorf("failed to convert inserted ID to ObjectID: %v", result.InsertedID)
	}

	doc.Id = oid
	return doc.Bucket, nil
}

func SelectBucketByName(ctx context.Context, db *mongo.Database, name string) (*storage.Bucket, error) {
	col := db.Collection(CollectionBuckets)
	var bucket BucketDocument
	err := col.FindOne(ctx, bson.M{
		"bucket.name": name,
		"deleted_at":  bson.M{"$exists": false},
	}).Decode(&bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to select bucket by name: %w", err)
	}
	return bucket.Bucket, nil
}

// SoftDeleteBucketByName marks a bucket as deleted by setting DeletedAt. A TTL
// index on this field is responsible for physically removing the document
// after a retention period.
func SoftDeleteBucketByName(ctx context.Context, db *mongo.Database, name string) error {
	col := db.Collection(CollectionBuckets)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{
			"bucket.name": name,
			"deleted_at":  bson.M{"$exists": false},
		},
		bson.M{
			"$set": bson.M{
				"deleted_at": now,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to soft delete bucket: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to soft delete bucket: %w", mongo.ErrNoDocuments)
	}
	return nil
}
