package cloudstorageemulator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nolanrsherman/gcpemulators/cloudstorageemulator/db"
	v1 "google.golang.org/genproto/googleapis/iam/v1"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/storage/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// storedObject holds both the object metadata and its data content
type storedObject struct {
	object *storage.Object
	data   []byte
}

// CloudStorageEmulator implements the Google Cloud Storage gRPC API
// using the public protobuf definitions from googleapis
type CloudStorageEmulator struct {
	port    int
	logger  *zap.Logger
	mongodb *mongo.Database
	// Embed the unimplemented server to satisfy the interface
	storage.UnimplementedStorageServer
}

func NewCloudStorageEmulator(port int, logger *zap.Logger, mongodb *mongo.Database) *CloudStorageEmulator {
	return &CloudStorageEmulator{
		port:    port,
		logger:  logger,
		mongodb: mongodb,
	}
}

func (e *CloudStorageEmulator) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(e.port))
	if err != nil {
		return fmt.Errorf("failed to listen on %d: %w", e.port, err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)
	defer grpcServer.GracefulStop()

	storage.RegisterStorageServer(grpcServer, e)

	// Channel to signal server completion
	serverDone := make(chan error, 1)

	// Start server in goroutine
	go func() {
		defer e.logger.Debug("goroutine stopped: grpcServer.Serve", zap.Int("port", e.port))
		err := grpcServer.Serve(lis)
		// Always signal completion, even on normal shutdown
		// grpc.ErrServerStopped is returned on normal shutdown, which is not an error
		if err != nil && err != grpc.ErrServerStopped && !errors.Is(err, context.Canceled) {
			serverDone <- err
		} else {
			serverDone <- nil
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		return nil

	case err := <-serverDone:
		// Server exited (error or normal)
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
		return nil
	}
}

// Permanently deletes an empty bucket.
func (e *CloudStorageEmulator) DeleteBucket(ctx context.Context, in *storage.DeleteBucketRequest) (*emptypb.Empty, error) {
	// Validate bucket name
	name := in.GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bucket name is required")
	}
	if err := validateBucketName(name); err != nil {
		return nil, err
	}

	// Perform soft delete
	if err := db.SoftDeleteBucketByName(ctx, e.mongodb, name); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "bucket %s not found", name)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete bucket: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// Returns metadata for the specified bucket.
func (e *CloudStorageEmulator) GetBucket(ctx context.Context, in *storage.GetBucketRequest) (*storage.Bucket, error) {
	bucket, err := db.SelectBucketByName(ctx, e.mongodb, in.Name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "bucket %s not found", in.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}
	return bucket, nil
}

// Creates a new bucket.
func (e *CloudStorageEmulator) CreateBucket(ctx context.Context, in *storage.CreateBucketRequest) (*storage.Bucket, error) {
	// Basic request validation based on the proto comments:
	// - Parent is required and must be of the form "projects/PROJECT_ID"
	// - Bucket is required
	// - BucketId is required and must be a valid bucket identifier
	// - bucket.name and bucket.project must NOT be populated by the caller
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if in.Bucket == nil {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}
	if in.Parent == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}
	if in.BucketId == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket_id is required")
	}

	// Validate parent and bucket id using the helpers used elsewhere.
	if err := validateProjectParent(in.Parent); err != nil {
		return nil, err
	}
	if err := validateBucketId(in.BucketId); err != nil {
		return nil, err
	}

	// Per proto comments: project and name are specified via parent and bucket_id.
	// Populating those fields inside bucket is an error.
	if in.Bucket.GetName() != "" {
		return nil, status.Error(codes.InvalidArgument, "bucket.name must not be set; use parent and bucket_id")
	}
	if in.Bucket.GetProject() != "" {
		return nil, status.Error(codes.InvalidArgument, "bucket.project must not be set; use parent")
	}

	// Build the canonical bucket resource name and validate it.
	// name = "projects/PROJECT_ID/buckets/BUCKET_ID"
	fullName := fmt.Sprintf("%s/buckets/%s", in.Parent, in.BucketId)
	if err := validateBucketName(fullName); err != nil {
		return nil, err
	}

	// Extract project ID from parent ("projects/PROJECT_ID").
	projectID := strings.TrimPrefix(in.Parent, "projects/")

	// Apply simple defaults for basic emulator behavior.
	location := in.Bucket.GetLocation()
	if location == "" {
		location = "us-central1"
	}
	storageClass := in.Bucket.GetStorageClass()
	if storageClass == "" {
		storageClass = "STANDARD"
	}

	// Construct the bucket that will be stored/returned.
	bucket := &storage.Bucket{
		Name:            fullName,
		BucketId:        in.BucketId,
		Project:         projectID,
		Location:        location,
		LocationType:    in.Bucket.GetLocationType(),
		StorageClass:    storageClass,
		Versioning:      in.Bucket.GetVersioning(),
		Lifecycle:       in.Bucket.GetLifecycle(),
		Labels:          in.Bucket.GetLabels(),
		Logging:         in.Bucket.GetLogging(),
		RetentionPolicy: in.Bucket.GetRetentionPolicy(),
		CreateTime:      timestamppb.New(time.Now()),
		UpdateTime:      timestamppb.New(time.Now()),
	}

	bucket, err := db.InsertBucket(ctx, e.mongodb, bucket)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create bucket: %v", err)
	}
	return bucket, nil
}

// Retrieves a list of buckets for a given project.
func (e *CloudStorageEmulator) ListBuckets(ctx context.Context, in *storage.ListBucketsRequest) (*storage.ListBucketsResponse, error) {
	// Validate parent (required)
	parent := in.GetParent()
	if parent == "" {
		return nil, status.Errorf(codes.InvalidArgument, "parent is required")
	}
	if err := validateProjectParent(parent); err != nil {
		return nil, err
	}

	// Extract project ID from parent (format: "projects/PROJECT_ID")
	projectID := strings.TrimPrefix(parent, "projects/")

	// Determine page size (clamp to maximum of 1000 per GCS API)
	pageSize := in.GetPageSize()
	if pageSize <= 0 {
		pageSize = 1000 // Default to maximum per GCS API
	}
	if pageSize > 1000 {
		pageSize = 1000
	}

	// Decode page token as an integer offset
	var offset int64
	if token := strings.TrimSpace(in.GetPageToken()); token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token")
		}
		offset = int64(n)
	}

	col := e.mongodb.Collection(db.CollectionBuckets)

	// Build filter: match project and exclude soft-deleted buckets
	filter := bson.M{
		"bucket.project": projectID,
		"deleted_at":     bson.M{"$exists": false},
	}

	// Add prefix filter if provided
	prefix := strings.TrimSpace(in.GetPrefix())
	if prefix != "" {
		// Filter buckets whose names begin with the prefix (case-sensitive)
		filter["bucket.name"] = bson.M{
			"$regex": "^" + regexp.QuoteMeta(prefix),
		}
	}

	// Fetch one extra document to determine if there is a next page
	limit := int64(pageSize) + 1
	opts := options.Find().
		SetSort(bson.D{{Key: "bucket.name", Value: 1}}). // Sort by bucket name
		SetSkip(offset).
		SetLimit(limit)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list buckets: %v", err)
	}
	defer cursor.Close(ctx)

	var dbBuckets []db.BucketDocument
	if err := cursor.All(ctx, &dbBuckets); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode buckets: %v", err)
	}

	hasMore := int32(len(dbBuckets)) > pageSize
	if hasMore {
		dbBuckets = dbBuckets[:pageSize]
	}

	res := &storage.ListBucketsResponse{
		Buckets:       make([]*storage.Bucket, 0, len(dbBuckets)),
		NextPageToken: "",
	}

	for _, doc := range dbBuckets {
		res.Buckets = append(res.Buckets, doc.Bucket)
	}

	if hasMore {
		res.NextPageToken = strconv.Itoa(int(offset) + len(res.Buckets))
	}

	return res, nil
}

// Locks retention policy on a bucket.
func (e *CloudStorageEmulator) LockBucketRetentionPolicy(ctx context.Context, in *storage.LockBucketRetentionPolicyRequest) (*storage.Bucket, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LockBucketRetentionPolicy not implemented")
}

// Gets the IAM policy for a specified bucket.
func (e *CloudStorageEmulator) GetIamPolicy(ctx context.Context, in *v1.GetIamPolicyRequest) (*v1.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetIamPolicy not implemented")
}

// Updates an IAM policy for the specified bucket.
func (e *CloudStorageEmulator) SetIamPolicy(ctx context.Context, in *v1.SetIamPolicyRequest) (*v1.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetIamPolicy not implemented")
}

// Tests a set of permissions on the given bucket to see which, if
// any, are held by the caller.
func (e *CloudStorageEmulator) TestIamPermissions(ctx context.Context, in *v1.TestIamPermissionsRequest) (*v1.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TestIamPermissions not implemented")
}

// Updates a bucket. Equivalent to JSON API's storage.buckets.patch method.
func (e *CloudStorageEmulator) UpdateBucket(ctx context.Context, in *storage.UpdateBucketRequest) (*storage.Bucket, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateBucket not implemented")
}

// Permanently deletes a notification subscription.
func (e *CloudStorageEmulator) DeleteNotification(ctx context.Context, in *storage.DeleteNotificationRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteNotification not implemented")
}

// View a notification config.
func (e *CloudStorageEmulator) GetNotification(ctx context.Context, in *storage.GetNotificationRequest) (*storage.Notification, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNotification not implemented")
}

// Creates a notification subscription for a given bucket.
// These notifications, when triggered, publish messages to the specified
// Pub/Sub topics.
// See https://cloud.google.com/storage/docs/pubsub-notifications.
func (e *CloudStorageEmulator) CreateNotification(ctx context.Context, in *storage.CreateNotificationRequest) (*storage.Notification, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateNotification not implemented")
}

// Retrieves a list of notification subscriptions for a given bucket.
func (e *CloudStorageEmulator) ListNotifications(ctx context.Context, in *storage.ListNotificationsRequest) (*storage.ListNotificationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListNotifications not implemented")
}

// Concatenates a list of existing objects into a new object in the same
// bucket.
func (e *CloudStorageEmulator) ComposeObject(ctx context.Context, in *storage.ComposeObjectRequest) (*storage.Object, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ComposeObject not implemented")
}

// Deletes an object and its metadata. Deletions are permanent if versioning
// is not enabled for the bucket, or if the `generation` parameter
// is used.
func (e *CloudStorageEmulator) DeleteObject(ctx context.Context, in *storage.DeleteObjectRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteObject not implemented")
}

// Retrieves an object's metadata.
func (e *CloudStorageEmulator) GetObject(ctx context.Context, in *storage.GetObjectRequest) (*storage.Object, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetObject not implemented")
}

// Reads an object's data.
func (e *CloudStorageEmulator) ReadObject(*storage.ReadObjectRequest, storage.Storage_ReadObjectServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadObject not implemented")
}

// Updates an object's metadata.
// Equivalent to JSON API's storage.objects.patch.
func (e *CloudStorageEmulator) UpdateObject(ctx context.Context, in *storage.UpdateObjectRequest) (*storage.Object, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateObject not implemented")
}

// Stores a new object and metadata.
//
// An object can be written either in a single message stream or in a
// resumable sequence of message streams. To write using a single stream,
// the client should include in the first message of the stream an
// `WriteObjectSpec` describing the destination bucket, object, and any
// preconditions. Additionally, the final message must set 'finish_write' to
// true, or else it is an error.
//
// For a resumable write, the client should instead call
// `StartResumableWrite()`, populating a `WriteObjectSpec` into that request.
// They should then attach the returned `upload_id` to the first message of
// each following call to `WriteObject`. If the stream is closed before
// finishing the upload (either explicitly by the client or due to a network
// error or an error response from the server), the client should do as
// follows:
//   - Check the result Status of the stream, to determine if writing can be
//     resumed on this stream or must be restarted from scratch (by calling
//     `StartResumableWrite()`). The resumable errors are DEADLINE_EXCEEDED,
//     INTERNAL, and UNAVAILABLE. For each case, the client should use binary
//     exponential backoff before retrying.  Additionally, writes can be
//     resumed after RESOURCE_EXHAUSTED errors, but only after taking
//     appropriate measures, which may include reducing aggregate send rate
//     across clients and/or requesting a quota increase for your project.
//   - If the call to `WriteObject` returns `ABORTED`, that indicates
//     concurrent attempts to update the resumable write, caused either by
//     multiple racing clients or by a single client where the previous
//     request was timed out on the client side but nonetheless reached the
//     server. In this case the client should take steps to prevent further
//     concurrent writes (e.g., increase the timeouts, stop using more than
//     one process to perform the upload, etc.), and then should follow the
//     steps below for resuming the upload.
//   - For resumable errors, the client should call `QueryWriteStatus()` and
//     then continue writing from the returned `persisted_size`. This may be
//     less than the amount of data the client previously sent. Note also that
//     it is acceptable to send data starting at an offset earlier than the
//     returned `persisted_size`; in this case, the service will skip data at
//     offsets that were already persisted (without checking that it matches
//     the previously written data), and write only the data starting from the
//     persisted offset. This behavior can make client-side handling simpler
//     in some cases.
//
// The service will not view the object as complete until the client has
// sent a `WriteObjectRequest` with `finish_write` set to `true`. Sending any
// requests on a stream after sending a request with `finish_write` set to
// `true` will cause an error. The client **should** check the response it
// receives to determine how much data the service was able to commit and
// whether the service views the object as complete.
//
// Attempting to resume an already finalized object will result in an OK
// status, with a WriteObjectResponse containing the finalized object's
// metadata.
func (e *CloudStorageEmulator) WriteObject(server storage.Storage_WriteObjectServer) error {
	return status.Errorf(codes.Unimplemented, "method WriteObject not implemented")
}

// Retrieves a list of objects matching the criteria.
func (e *CloudStorageEmulator) ListObjects(ctx context.Context, in *storage.ListObjectsRequest) (*storage.ListObjectsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListObjects not implemented")
}

// Rewrites a source object to a destination object. Optionally overrides
// metadata.
func (e *CloudStorageEmulator) RewriteObject(ctx context.Context, in *storage.RewriteObjectRequest) (*storage.RewriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RewriteObject not implemented")
}

// Starts a resumable write. How long the write operation remains valid, and
// what happens when the write operation becomes invalid, are
// service-dependent.
func (e *CloudStorageEmulator) StartResumableWrite(ctx context.Context, in *storage.StartResumableWriteRequest) (*storage.StartResumableWriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartResumableWrite not implemented")
}

// Determines the `persisted_size` for an object that is being written, which
// can then be used as the `write_offset` for the next `Write()` call.
//
// If the object does not exist (i.e., the object has been deleted, or the
// first `Write()` has not yet reached the service), this method returns the
// error `NOT_FOUND`.
//
// The client **may** call `QueryWriteStatus()` at any time to determine how
// much data has been processed for this object. This is useful if the
// client is buffering data and needs to know which data can be safely
// evicted. For any sequence of `QueryWriteStatus()` calls for a given
// object name, the sequence of returned `persisted_size` values will be
// non-decreasing.
func (e *CloudStorageEmulator) QueryWriteStatus(ctx context.Context, in *storage.QueryWriteStatusRequest) (*storage.QueryWriteStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryWriteStatus not implemented")
}

// Retrieves the name of a project's Google Cloud Storage service account.
func (e *CloudStorageEmulator) GetServiceAccount(ctx context.Context, in *storage.GetServiceAccountRequest) (*storage.ServiceAccount, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetServiceAccount not implemented")
}

// Creates a new HMAC key for the given service account.
func (e *CloudStorageEmulator) CreateHmacKey(ctx context.Context, in *storage.CreateHmacKeyRequest) (*storage.CreateHmacKeyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateHmacKey not implemented")
}

// Deletes a given HMAC key.  Key must be in an INACTIVE state.
func (e *CloudStorageEmulator) DeleteHmacKey(ctx context.Context, in *storage.DeleteHmacKeyRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteHmacKey not implemented")
}

// Gets an existing HMAC key metadata for the given id.
func (e *CloudStorageEmulator) GetHmacKey(ctx context.Context, in *storage.GetHmacKeyRequest) (*storage.HmacKeyMetadata, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHmacKey not implemented")
}

// Lists HMAC keys under a given project with the additional filters provided.
func (e *CloudStorageEmulator) ListHmacKeys(ctx context.Context, in *storage.ListHmacKeysRequest) (*storage.ListHmacKeysResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListHmacKeys not implemented")
}

// Updates a given HMAC key state between ACTIVE and INACTIVE.
func (e *CloudStorageEmulator) UpdateHmacKey(ctx context.Context, in *storage.UpdateHmacKeyRequest) (*storage.HmacKeyMetadata, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateHmacKey not implemented")
}

// should be in the format projects/PROJECT_ID
// PROJECT_ID should be a valid alphanumeric string
// it must start with projects/ and can only be followed by a valid project id format.
func validateProjectParent(parent string) error {
	if !strings.HasPrefix(parent, "projects/") {
		return status.Errorf(codes.InvalidArgument, "invalid project parent: %s", parent)
	}
	projectID := strings.TrimPrefix(parent, "projects/")
	if projectID == "" {
		return status.Errorf(codes.InvalidArgument, "project ID is required")
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9\-:.]+$`).MatchString(projectID) {
		return status.Errorf(codes.InvalidArgument, "project ID must be a valid format: %s", projectID)
	}
	return nil
}

func validateBucketId(bucketId string) error {
	if bucketId == "" {
		return status.Errorf(codes.InvalidArgument, "bucket name is required")
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9\-]+$`).MatchString(bucketId) {
		return status.Errorf(codes.InvalidArgument, "bucket name must be a valid format: %s", bucketId)
	}
	return nil
}

func validateBucketName(name string) error {
	parts := strings.Split(name, "/buckets/")
	if len(parts) != 2 {
		return status.Errorf(codes.InvalidArgument, "bucket name must be in the format projects/PROJECT_ID/buckets/BUCKET_ID")
	}
	parent := parts[0]
	if err := validateProjectParent(parent); err != nil {
		return status.Errorf(codes.InvalidArgument, "project parent must be a valid format: %s", parent)
	}
	bucketId := parts[1]
	if err := validateBucketId(bucketId); err != nil {
		return status.Errorf(codes.InvalidArgument, "bucket ID must be a valid format: %s", bucketId)
	}
	return nil
}
