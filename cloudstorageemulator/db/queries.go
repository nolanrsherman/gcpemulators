package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"google.golang.org/genproto/googleapis/storage/v2"
)

const (
	CollectionBuckets = "cloudstorageemulator_buckets"
	CollectionObjects = "cloudstorageemulator_objects"
	CollectionUploads = "cloudstorageemulator_uploads"
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

func SelectBucketByID(ctx context.Context, db *mongo.Database, id string) (*storage.Bucket, error) {
	col := db.Collection(CollectionBuckets)
	var bucket BucketDocument
	err := col.FindOne(ctx, bson.M{
		"bucket.bucket_id": id,
		"deleted_at":       bson.M{"$exists": false},
	}).Decode(&bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to select bucket by ID: %w", err)
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

func UploadWithGridFs(ctx context.Context, db *mongo.Database, fileId, fileName string) (*gridfs.UploadStream, error) {
	bucket, err := gridfs.NewBucket(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create gridfs bucket: %w", err)
	}
	uploadStream, err := bucket.OpenUploadStreamWithID(fileId, fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open upload stream: %w", err)
	}
	return uploadStream, nil
}

// InsertUpload creates a new upload tracking document for resumable writes
func InsertUpload(ctx context.Context, db *mongo.Database, bucket, objectName string, spec *storage.WriteObjectSpec) (string, error) {
	col := db.Collection(CollectionUploads)
	now := time.Now()
	uploadID := primitive.NewObjectID().Hex()

	doc := &UploadDocument{
		UploadID:      uploadID,
		Bucket:        bucket,
		ObjectName:    objectName,
		Spec:          spec,
		PersistedSize: 0,
		Completed:     false,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	result, err := col.InsertOne(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("failed to insert upload: %w", err)
	}

	oid, ok := result.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", fmt.Errorf("failed to convert inserted ID to ObjectID: %v", result.InsertedID)
	}
	doc.Id = oid
	return uploadID, nil
}

// SelectUploadByID retrieves an upload by its upload ID
func SelectUploadByID(ctx context.Context, db *mongo.Database, uploadID string) (*UploadDocument, error) {
	col := db.Collection(CollectionUploads)
	var upload UploadDocument
	err := col.FindOne(ctx, bson.M{"upload_id": uploadID}).Decode(&upload)
	if err != nil {
		return nil, fmt.Errorf("failed to select upload by ID: %w", err)
	}
	return &upload, nil
}

// UpdateUploadPersistedSize updates the persisted size for an upload
func UpdateUploadPersistedSize(ctx context.Context, db *mongo.Database, uploadID string, persistedSize int64) error {
	col := db.Collection(CollectionUploads)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{"upload_id": uploadID},
		bson.M{
			"$set": bson.M{
				"persisted_size": persistedSize,
				"updated_at":     now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update upload persisted size: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to update upload: %w", mongo.ErrNoDocuments)
	}
	return nil
}

// CompleteUpload marks an upload as completed
func CompleteUpload(ctx context.Context, db *mongo.Database, uploadID string) error {
	col := db.Collection(CollectionUploads)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{"upload_id": uploadID},
		bson.M{
			"$set": bson.M{
				"completed":  true,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to complete upload: %w", mongo.ErrNoDocuments)
	}
	return nil
}

// InsertObject stores object metadata
func InsertObject(ctx context.Context, db *mongo.Database, object *storage.Object, gridFSFileID primitive.ObjectID) error {
	col := db.Collection(CollectionObjects)
	now := time.Now()

	doc := &ObjectDocument{
		Object:       object,
		GridFSFileID: gridFSFileID,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	_, err := col.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert object: %w", err)
	}
	return nil
}

// SelectObjectByBucketAndName retrieves an object by bucket and object name
func SelectObjectByBucketAndName(ctx context.Context, db *mongo.Database, bucket, objectName string) (*storage.Object, error) {
	objDoc, err := SelectObjectDocumentByBucketAndName(ctx, db, bucket, objectName)
	if err != nil {
		return nil, err
	}
	return objDoc.Object, nil
}

// SelectObjectDocumentByBucketAndName retrieves an ObjectDocument by bucket and object name
func SelectObjectDocumentByBucketAndName(ctx context.Context, db *mongo.Database, bucket, objectName string) (*ObjectDocument, error) {
	col := db.Collection(CollectionObjects)
	var objDoc ObjectDocument
	err := col.FindOne(ctx, bson.M{
		"object.bucket": bucket,
		"object.name":   objectName,
		"deleted_at":    bson.M{"$exists": false},
	}).Decode(&objDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to select object by bucket and name: %w", err)
	}
	return &objDoc, nil
}

// SelectObjectByBucketNameAndGeneration retrieves an object by bucket, name, and generation
func SelectObjectByBucketNameAndGeneration(ctx context.Context, db *mongo.Database, bucket, objectName string, generation int64) (*storage.Object, error) {
	objDoc, err := SelectObjectDocumentByBucketNameAndGeneration(ctx, db, bucket, objectName, generation)
	if err != nil {
		return nil, err
	}
	return objDoc.Object, nil
}

// SelectObjectDocumentByBucketNameAndGeneration retrieves an ObjectDocument by bucket, name, and generation
func SelectObjectDocumentByBucketNameAndGeneration(ctx context.Context, db *mongo.Database, bucket, objectName string, generation int64) (*ObjectDocument, error) {
	col := db.Collection(CollectionObjects)
	var objDoc ObjectDocument
	err := col.FindOne(ctx, bson.M{
		"object.bucket":     bucket,
		"object.name":       objectName,
		"object.generation": generation,
		"deleted_at":        bson.M{"$exists": false},
	}).Decode(&objDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to select object by bucket, name, and generation: %w", err)
	}
	return &objDoc, nil
}

// SoftDeleteObjectByBucketAndName marks an object as deleted by setting DeletedAt
func SoftDeleteObjectByBucketAndName(ctx context.Context, db *mongo.Database, bucket, objectName string) error {
	col := db.Collection(CollectionObjects)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{
			"object.bucket": bucket,
			"object.name":   objectName,
			"deleted_at":    bson.M{"$exists": false},
		},
		bson.M{
			"$set": bson.M{
				"deleted_at": now,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to soft delete object: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to soft delete object: %w", mongo.ErrNoDocuments)
	}
	return nil
}

// SoftDeleteObjectByBucketNameAndGeneration marks a specific generation of an object as deleted
func SoftDeleteObjectByBucketNameAndGeneration(ctx context.Context, db *mongo.Database, bucket, objectName string, generation int64) error {
	col := db.Collection(CollectionObjects)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{
			"object.bucket":     bucket,
			"object.name":       objectName,
			"object.generation": generation,
			"deleted_at":        bson.M{"$exists": false},
		},
		bson.M{
			"$set": bson.M{
				"deleted_at": now,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to soft delete object: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to soft delete object: %w", mongo.ErrNoDocuments)
	}
	return nil
}

// UpdateUploadFileID updates the GridFS file ID for an upload
func UpdateUploadFileID(ctx context.Context, db *mongo.Database, uploadID string, fileID primitive.ObjectID) error {
	col := db.Collection(CollectionUploads)
	now := time.Now()

	res, err := col.UpdateOne(ctx,
		bson.M{"upload_id": uploadID},
		bson.M{
			"$set": bson.M{
				"gridfs_file_id": fileID,
				"updated_at":     now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to update upload file ID: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("failed to update upload: %w", mongo.ErrNoDocuments)
	}
	return nil
}
