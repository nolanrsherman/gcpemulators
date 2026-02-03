package db

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/genproto/googleapis/storage/v2"
)

type BucketDocument struct {
	Id        primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	CreatedAt time.Time          `bson:"created_at,omitempty" json:"created_at,omitempty"`
	UpdatedAt time.Time          `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
	DeletedAt *time.Time         `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
	Bucket    *storage.Bucket    `bson:"bucket" json:"bucket"`
}
