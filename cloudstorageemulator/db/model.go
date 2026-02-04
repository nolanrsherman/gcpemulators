package db

import (
	"time"

	storage "github.com/nolanrsherman/gcpemulators/cloudstorageemulator/storagepb"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BucketDocument struct {
	Id        primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	CreatedAt time.Time          `bson:"created_at,omitempty" json:"created_at,omitempty"`
	UpdatedAt time.Time          `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
	DeletedAt *time.Time         `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
	Bucket    *storage.Bucket    `bson:"bucket" json:"bucket"`
}

// UploadDocument tracks resumable uploads
type UploadDocument struct {
	Id            primitive.ObjectID       `bson:"_id,omitempty" json:"_id,omitempty"`
	UploadID      string                   `bson:"upload_id" json:"upload_id"`
	Bucket        string                   `bson:"bucket" json:"bucket"`
	ObjectName    string                   `bson:"object_name" json:"object_name"`
	Spec          *storage.WriteObjectSpec `bson:"spec" json:"spec"`
	PersistedSize int64                    `bson:"persisted_size" json:"persisted_size"`
	GridFSFileID  primitive.ObjectID       `bson:"gridfs_file_id,omitempty" json:"gridfs_file_id,omitempty"`
	Completed     bool                     `bson:"completed" json:"completed"`
	CreatedAt     time.Time                `bson:"created_at" json:"created_at"`
	UpdatedAt     time.Time                `bson:"updated_at" json:"updated_at"`
}

// ObjectDocument stores object metadata
type ObjectDocument struct {
	Id           primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Object       *storage.Object    `bson:"object" json:"object"`
	GridFSFileID primitive.ObjectID `bson:"gridfs_file_id" json:"gridfs_file_id"`
	CreatedAt    time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt    time.Time          `bson:"updated_at" json:"updated_at"`
	DeletedAt    *time.Time         `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
}
