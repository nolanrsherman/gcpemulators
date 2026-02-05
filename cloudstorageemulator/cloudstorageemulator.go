package cloudstorageemulator

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/nolanrsherman/gcpemulators/cloudstorageemulator/db"
	storage "github.com/nolanrsherman/gcpemulators/cloudstorageemulator/storagepb"
	"github.com/nolanrsherman/gcpemulators/gcpemulators/gcpemulatorspb"
	v1 "google.golang.org/genproto/googleapis/iam/v1"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// crc32cTable uses the Castagnoli polynomial, which matches GCS CRC32C
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

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
	gcpemulatorspb.UnimplementedGcpEmulatorServer
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
		grpc.ChainUnaryInterceptor(
			grpc_zap.UnaryServerInterceptor(e.logger),
		),
		grpc.ChainStreamInterceptor(
			grpc_zap.StreamServerInterceptor(e.logger),
		),
	)
	defer grpcServer.GracefulStop()

	storage.RegisterStorageServer(grpcServer, e)
	gcpemulatorspb.RegisterGcpEmulatorServer(grpcServer, e)

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

func (e *CloudStorageEmulator) Readiness(context.Context, *gcpemulatorspb.ReadinessRequest) (*gcpemulatorspb.ReadinessResponse, error) {
	return &gcpemulatorspb.ReadinessResponse{
		Ready:   true,
		Message: "Emulator is ready and accepting requests",
	}, nil
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

// Concatenates a list of existing objects into a new object in the same
// bucket.
func (e *CloudStorageEmulator) ComposeObject(ctx context.Context, in *storage.ComposeObjectRequest) (*storage.Object, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ComposeObject not implemented")
}

// Deletes an object and its metadata. Deletions are permanent if versioning
// is not enabled for the bucket, or if the `generation` parameter
// is used.
func (e *CloudStorageEmulator) DeleteObject(ctx context.Context, in *storage.DeleteObjectRequest) (*emptypb.Empty, error) {
	// Validate request
	if in == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	if in.Bucket == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bucket is required")
	}
	if in.Object == "" {
		return nil, status.Errorf(codes.InvalidArgument, "object name is required")
	}

	// Validate bucket exists
	_, err := db.SelectBucketByID(ctx, e.mongodb, in.Bucket)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "bucket %s not found", in.Bucket)
		}
		return nil, status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}

	// Get object document to check preconditions and get GridFSFileID
	var objDoc *db.ObjectDocument
	if in.Generation != 0 {
		// Get specific generation
		objDoc, err = db.SelectObjectDocumentByBucketNameAndGeneration(ctx, e.mongodb, in.Bucket, in.Object, in.Generation)
	} else {
		// Get latest version
		objDoc, err = db.SelectObjectDocumentByBucketAndName(ctx, e.mongodb, in.Bucket, in.Object)
	}

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "object %s not found in bucket %s", in.Object, in.Bucket)
		}
		return nil, status.Errorf(codes.Internal, "failed to get object: %v", err)
	}

	object := objDoc.Object

	// Check generation match precondition
	if in.IfGenerationMatch != nil {
		if object.Generation != *in.IfGenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition, "object generation %d does not match expected %d", object.Generation, *in.IfGenerationMatch)
		}
	}

	// Check generation not match precondition
	if in.IfGenerationNotMatch != nil {
		if object.Generation == *in.IfGenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition, "object generation %d matches the excluded value", object.Generation)
		}
	}

	// Check metageneration match precondition
	if in.IfMetagenerationMatch != nil {
		if object.Metageneration != *in.IfMetagenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition, "object metageneration %d does not match expected %d", object.Metageneration, *in.IfMetagenerationMatch)
		}
	}

	// Check metageneration not match precondition
	if in.IfMetagenerationNotMatch != nil {
		if object.Metageneration == *in.IfMetagenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition, "object metageneration %d matches the excluded value", object.Metageneration)
		}
	}

	// Soft delete the object metadata
	if in.Generation != 0 {
		err = db.SoftDeleteObjectByBucketNameAndGeneration(ctx, e.mongodb, in.Bucket, in.Object, in.Generation)
	} else {
		err = db.SoftDeleteObjectByBucketAndName(ctx, e.mongodb, in.Bucket, in.Object)
	}
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "object %s not found in bucket %s", in.Object, in.Bucket)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete object: %v", err)
	}

	// Delete the GridFS file
	bucketOpts := options.GridFSBucket().SetName("cloudstorageemulator_objects")
	gridFSBucket, err := gridfs.NewBucket(e.mongodb, bucketOpts)
	if err != nil {
		// Log error but don't fail - metadata is already deleted
		e.logger.Warn("failed to create GridFS bucket for file deletion", zap.Error(err))
	} else {
		err = gridFSBucket.Delete(objDoc.GridFSFileID)
		if err != nil {
			// Log error but don't fail - metadata is already deleted
			// GridFS file will be orphaned but that's acceptable for an emulator
			e.logger.Warn("failed to delete GridFS file", zap.Error(err), zap.String("file_id", objDoc.GridFSFileID.Hex()))
		}
	}

	return &emptypb.Empty{}, nil
}

// Retrieves an object's metadata.
func (e *CloudStorageEmulator) GetObject(ctx context.Context, in *storage.GetObjectRequest) (*storage.Object, error) {
	// Validate request
	if in == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request is required")
	}
	if in.Bucket == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bucket is required")
	}
	if in.Object == "" {
		return nil, status.Errorf(codes.InvalidArgument, "object name is required")
	}

	// Validate bucket exists
	_, err := db.SelectBucketByName(ctx, e.mongodb, in.Bucket)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "bucket %s not found", in.Bucket)
		}
		return nil, status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}

	var object *storage.Object

	// If generation is specified, query by generation
	if in.Generation != 0 {
		object, err = db.SelectObjectByBucketNameAndGeneration(ctx, e.mongodb, in.Bucket, in.Object, in.Generation)
	} else {
		// Otherwise, get the latest version (for now, just get any matching object)
		// TODO: Implement versioning to get the latest generation
		object, err = db.SelectObjectByBucketAndName(ctx, e.mongodb, in.Bucket, in.Object)
	}

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "object %s not found in bucket %s", in.Object, in.Bucket)
		}
		return nil, status.Errorf(codes.Internal, "failed to get object: %v", err)
	}

	// Check generation match precondition if specified
	if in.IfGenerationMatch != nil {
		if object.Generation != *in.IfGenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition, "object generation %d does not match expected %d", object.Generation, *in.IfGenerationMatch)
		}
	}

	// Check generation not match precondition if specified
	if in.IfGenerationNotMatch != nil {
		if object.Generation == *in.IfGenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition, "object generation %d matches the excluded value", object.Generation)
		}
	}

	return object, nil
}

// Reads an object's data.
func (e *CloudStorageEmulator) ReadObject(req *storage.ReadObjectRequest, server storage.Storage_ReadObjectServer) error {
	ctx := server.Context()

	// Validate request
	if req == nil {
		return status.Errorf(codes.InvalidArgument, "request is required")
	}
	if req.Bucket == "" {
		return status.Errorf(codes.InvalidArgument, "bucket is required")
	}
	if req.Object == "" {
		return status.Errorf(codes.InvalidArgument, "object name is required")
	}

	// Validate bucket exists
	_, err := db.SelectBucketByName(ctx, e.mongodb, req.Bucket)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return status.Errorf(codes.NotFound, "bucket %s not found", req.Bucket)
		}
		return status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}

	// Get object document (need GridFSFileID)
	objDoc, err := db.SelectObjectDocumentByBucketAndName(ctx, e.mongodb, req.Bucket, req.Object)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return status.Errorf(codes.NotFound, "object %s not found in bucket %s", req.Object, req.Bucket)
		}
		return status.Errorf(codes.Internal, "failed to get object: %v", err)
	}

	object := objDoc.Object

	// Check generation preconditions if specified
	if req.IfGenerationMatch != nil {
		if object.Generation != *req.IfGenerationMatch {
			return status.Errorf(codes.FailedPrecondition, "object generation %d does not match expected %d", object.Generation, *req.IfGenerationMatch)
		}
	}
	if req.IfGenerationNotMatch != nil {
		if object.Generation == *req.IfGenerationNotMatch {
			return status.Errorf(codes.FailedPrecondition, "object generation %d matches the excluded value", object.Generation)
		}
	}

	// Get GridFS bucket
	bucketOpts := options.GridFSBucket().SetName("cloudstorageemulator_objects")
	gridFSBucket, err := gridfs.NewBucket(e.mongodb, bucketOpts)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create GridFS bucket: %v", err)
	}

	// Open download stream
	downloadStream, err := gridFSBucket.OpenDownloadStream(objDoc.GridFSFileID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to open GridFS download stream: %v", err)
	}
	defer downloadStream.Close()

	// Handle read_offset
	readOffset := req.ReadOffset
	if readOffset < 0 {
		// Negative offset means from the end
		fileSize := object.Size
		readOffset = fileSize + readOffset
		if readOffset < 0 {
			readOffset = 0
		}
	}

	// Read and discard data up to the offset (GridFS doesn't support seeking)
	if readOffset > 0 {
		discardBuffer := make([]byte, 1024*1024) // 1MB buffer for discarding
		discarded := int64(0)
		for discarded < readOffset {
			toDiscard := readOffset - discarded
			if toDiscard > int64(len(discardBuffer)) {
				toDiscard = int64(len(discardBuffer))
			}
			n, err := downloadStream.Read(discardBuffer[:toDiscard])
			if err != nil && err != io.EOF {
				return status.Errorf(codes.Internal, "failed to read to offset: %v", err)
			}
			if n == 0 {
				break
			}
			discarded += int64(n)
			if err == io.EOF {
				break
			}
		}
		if discarded < readOffset {
			return status.Errorf(codes.InvalidArgument, "read_offset %d exceeds file size", readOffset)
		}
	}

	// Calculate read limit
	readLimit := req.ReadLimit
	if readLimit < 0 {
		return status.Errorf(codes.InvalidArgument, "read_limit cannot be negative")
	}
	if readLimit == 0 {
		// No limit, read until end
		readLimit = object.Size - readOffset
		if readLimit < 0 {
			readLimit = 0
		}
	}

	// Calculate actual bytes to read
	bytesToRead := readLimit
	if readOffset+bytesToRead > object.Size {
		bytesToRead = object.Size - readOffset
		if bytesToRead < 0 {
			bytesToRead = 0
		}
	}

	// Prepare first response with metadata
	firstResponse := &storage.ReadObjectResponse{
		Metadata: object,
	}

	// Add ContentRange if offset or limit was specified
	if req.ReadOffset != 0 || req.ReadLimit != 0 {
		firstResponse.ContentRange = &storage.ContentRange{
			Start:          readOffset,
			End:            readOffset + bytesToRead - 1,
			CompleteLength: object.Size,
		}
	}

	// Add object checksums if available
	if object.Checksums != nil {
		firstResponse.ObjectChecksums = object.Checksums
	}

	// Send first response with metadata
	err = server.Send(firstResponse)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to send first response: %v", err)
	}

	// Stream data in chunks
	chunkSize := int64(1024 * 1024) // 1MB chunks
	bytesRead := int64(0)
	buffer := make([]byte, chunkSize)

	for bytesRead < bytesToRead {
		// Calculate how much to read in this chunk
		remaining := bytesToRead - bytesRead
		toRead := chunkSize
		if remaining < chunkSize {
			toRead = remaining
		}

		n, err := downloadStream.Read(buffer[:toRead])
		if err != nil && err != io.EOF {
			return status.Errorf(codes.Internal, "failed to read from GridFS: %v", err)
		}
		if n == 0 {
			break
		}

		// Send data chunk
		response := &storage.ReadObjectResponse{
			ChecksummedData: &storage.ChecksummedData{
				Content: buffer[:n],
			},
		}

		err = server.Send(response)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to send data chunk: %v", err)
		}

		bytesRead += int64(n)
		if err == io.EOF {
			break
		}
	}

	return nil
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
	ctx := server.Context()

	// Receive first message to determine if this is resumable or non-resumable
	firstReq, err := server.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Errorf(codes.InvalidArgument, "no data received")
		}
		return status.Errorf(codes.Internal, "failed to receive first message: %v", err)
	}

	var uploadID string
	var spec *storage.WriteObjectSpec
	var bucketName, objectName string

	// Determine if this is a resumable or non-resumable write
	switch msg := firstReq.FirstMessage.(type) {
	case *storage.WriteObjectRequest_UploadId:
		uploadID = msg.UploadId
		// Load upload metadata to get bucket and object name
		upload, err := db.SelectUploadByID(ctx, e.mongodb, uploadID)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return status.Errorf(codes.NotFound, "upload_id not found: %s", uploadID)
			}
			return status.Errorf(codes.Internal, "failed to get upload: %v", err)
		}
		bucketName = upload.Bucket
		objectName = upload.ObjectName
		spec = upload.Spec
	case *storage.WriteObjectRequest_WriteObjectSpec:
		spec = msg.WriteObjectSpec
		if spec == nil || spec.Resource == nil {
			return status.Errorf(codes.InvalidArgument, "WriteObjectSpec is required for non-resumable writes")
		}
		bucketName = spec.Resource.Bucket
		objectName = spec.Resource.Name
		if bucketName == "" || objectName == "" {
			return status.Errorf(codes.InvalidArgument, "bucket and object name are required")
		}
		// Create upload for tracking
		uploadID, err = db.InsertUpload(ctx, e.mongodb, bucketName, objectName, spec)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create upload: %v", err)
		}
	default:
		return status.Errorf(codes.InvalidArgument, "first message must contain either upload_id or write_object_spec")
	}

	// Validate bucket exists
	_, err = db.SelectBucketByID(ctx, e.mongodb, bucketName)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return status.Errorf(codes.NotFound, "bucket %s not found", bucketName)
		}
		return status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}

	// Get or create GridFS bucket
	bucketOpts := options.GridFSBucket().SetName("cloudstorageemulator_objects")
	gridFSBucket, err := gridfs.NewBucket(e.mongodb, bucketOpts)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create GridFS bucket: %v", err)
	}

	// Determine write offset
	writeOffset := firstReq.WriteOffset
	if writeOffset < 0 {
		return status.Errorf(codes.InvalidArgument, "write_offset must be non-negative")
	}

	// Get current upload state
	upload, err := db.SelectUploadByID(ctx, e.mongodb, uploadID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get upload state: %v", err)
	}

	// For resumable writes, validate offset matches persisted size
	if writeOffset > 0 && writeOffset != upload.PersistedSize {
		return status.Errorf(codes.InvalidArgument, "write_offset %d does not match persisted_size %d", writeOffset, upload.PersistedSize)
	}

	// Prepare GridFS upload stream
	var uploadStream *gridfs.UploadStream
	var fileID primitive.ObjectID
	filename := fmt.Sprintf("%s/%s", bucketName, objectName)

	if writeOffset == 0 {
		// Starting fresh - create new upload stream
		uploadStream, err = gridFSBucket.OpenUploadStream(filename)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to open GridFS upload stream: %v", err)
		}
		fileID, ok := uploadStream.FileID.(primitive.ObjectID)
		if !ok {
			uploadStream.Close()
			return status.Errorf(codes.Internal, "failed to convert FileID to ObjectID")
		}
		upload.GridFSFileID = fileID
		err = db.UpdateUploadFileID(ctx, e.mongodb, uploadID, fileID)
		if err != nil {
			uploadStream.Close()
			return status.Errorf(codes.Internal, "failed to update upload file ID: %v", err)
		}
	} else {
		// Resume existing upload - open stream with existing file ID
		fileID = upload.GridFSFileID
		if fileID.IsZero() {
			return status.Errorf(codes.InvalidArgument, "cannot resume upload without file ID")
		}
		uploadStream, err = gridFSBucket.OpenUploadStreamWithID(fileID, filename)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resume GridFS upload: %v", err)
		}
		// For resumable uploads, we need to read existing data to validate checksums
		// Note: GridFS doesn't support seeking, so we read the existing file
		downloadStream, err := gridFSBucket.OpenDownloadStream(fileID)
		if err == nil {
			existingData := make([]byte, writeOffset)
			_, err = downloadStream.Read(existingData)
			downloadStream.Close()
			if err != nil && err != io.EOF {
				uploadStream.Close()
				return status.Errorf(codes.Internal, "failed to read existing file data: %v", err)
			}
		}
	}

	defer uploadStream.Close()

	// Process data chunks and write to GridFS
	var totalWritten int64 = writeOffset
	var allData []byte
	var finished bool
	var expectedCrc32c *uint32
	var expectedMd5Hash []byte
	hasFirstMessage := true

	// Load existing data if resuming (for checksum validation)
	if writeOffset > 0 {
		downloadStream, err := gridFSBucket.OpenDownloadStream(fileID)
		if err == nil {
			existingData := make([]byte, writeOffset)
			_, err = downloadStream.Read(existingData)
			downloadStream.Close()
			if err != nil && err != io.EOF {
				return status.Errorf(codes.Internal, "failed to read existing file data: %v", err)
			}
			allData = existingData
		}
	}

	// Process data chunks
	for {
		var req *storage.WriteObjectRequest
		if hasFirstMessage {
			req = firstReq
			hasFirstMessage = false
		} else {
			var recvErr error
			req, recvErr = server.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return status.Errorf(codes.Internal, "failed to receive message: %v", recvErr)
			}
		}

		// Validate write offset
		if req.WriteOffset != totalWritten {
			return status.Errorf(codes.InvalidArgument, "write_offset mismatch: expected %d, got %d", totalWritten, req.WriteOffset)
		}

		// Process data chunk
		if checksummedData := req.GetChecksummedData(); checksummedData != nil {
			data := checksummedData.Content

			// Validate CRC32C if provided (GCS uses CRC32C Castagnoli)
			if checksummedData.Crc32C != nil {
				calculated := crc32.Checksum(data, crc32cTable)
				if calculated != *checksummedData.Crc32C {
					return status.Errorf(codes.InvalidArgument, "CRC32C checksum mismatch")
				}
			}

			// Write to GridFS stream
			n, err := uploadStream.Write(data)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to write to GridFS: %v", err)
			}
			totalWritten += int64(n)
			allData = append(allData, data...)
		}

		// Check for object-level checksums (only in first or last message)
		if req.ObjectChecksums != nil {
			if req.ObjectChecksums.Crc32C != nil {
				expectedCrc32c = req.ObjectChecksums.Crc32C
			}
			if len(req.ObjectChecksums.Md5Hash) > 0 {
				expectedMd5Hash = req.ObjectChecksums.Md5Hash
			}
		}

		// Check if write is finished
		if req.FinishWrite {
			finished = true
			break
		}
	}

	// Update persisted size
	err = db.UpdateUploadPersistedSize(ctx, e.mongodb, uploadID, totalWritten)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to update persisted size: %v", err)
	}

	// If not finished, return persisted size
	if !finished {
		return server.SendAndClose(&storage.WriteObjectResponse{
			WriteStatus: &storage.WriteObjectResponse_PersistedSize{
				PersistedSize: totalWritten,
			},
		})
	}

	// Close the upload stream to finalize the file
	err = uploadStream.Close()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to close GridFS upload stream: %v", err)
	}

	// Validate final checksums if provided
	if expectedCrc32c != nil {
		calculated := crc32.Checksum(allData, crc32cTable)
		if calculated != *expectedCrc32c {
			return status.Errorf(codes.InvalidArgument, "object CRC32C checksum mismatch")
		}
	}
	if len(expectedMd5Hash) > 0 {
		hash := md5.Sum(allData)
		if string(hash[:]) != string(expectedMd5Hash) {
			return status.Errorf(codes.InvalidArgument, "object MD5 checksum mismatch")
		}
	}

	// Create object metadata
	now := time.Now()
	object := &storage.Object{
		Name:           objectName,
		Bucket:         bucketName,
		Size:           totalWritten,
		Generation:     time.Now().UnixNano(), // Simple generation based on timestamp
		Metageneration: 1,
		CreateTime:     timestamppb.New(now),
		UpdateTime:     timestamppb.New(now),
		ContentType:    spec.Resource.ContentType,
	}

	// Set checksums in object
	if expectedCrc32c != nil {
		object.Checksums = &storage.ObjectChecksums{
			Crc32C: expectedCrc32c,
		}
	}
	if len(expectedMd5Hash) > 0 {
		if object.Checksums == nil {
			object.Checksums = &storage.ObjectChecksums{}
		}
		object.Checksums.Md5Hash = expectedMd5Hash
	}

	// Store object metadata
	err = db.InsertObject(ctx, e.mongodb, object, upload.GridFSFileID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to store object metadata: %v", err)
	}

	// Mark upload as completed
	err = db.CompleteUpload(ctx, e.mongodb, uploadID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to complete upload: %v", err)
	}

	// Return final object
	return server.SendAndClose(&storage.WriteObjectResponse{
		WriteStatus: &storage.WriteObjectResponse_Resource{
			Resource: object,
		},
	})
}

// Retrieves a list of objects matching the criteria.
func (e *CloudStorageEmulator) ListObjects(ctx context.Context, in *storage.ListObjectsRequest) (*storage.ListObjectsResponse, error) {
	// Validate parent (required)
	parent := in.GetParent()
	if parent == "" {
		return nil, status.Errorf(codes.InvalidArgument, "parent is required")
	}

	// Extract bucket ID from parent
	// Parent can be either:
	// 1. Just the bucket ID (e.g., "my-bucket")
	// 2. Full resource name (e.g., "projects/PROJECT_ID/buckets/BUCKET_ID")
	bucketID := parent
	if strings.HasPrefix(parent, "projects/") {
		// Extract bucket ID from full resource name
		parts := strings.Split(parent, "/")
		if len(parts) >= 4 && parts[2] == "buckets" {
			bucketID = parts[3]
		} else {
			return nil, status.Errorf(codes.InvalidArgument, "invalid parent format: %s", parent)
		}
	}

	// Validate bucket exists
	_, err := db.SelectBucketByID(ctx, e.mongodb, bucketID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "bucket %s not found", bucketID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}

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

	col := e.mongodb.Collection(db.CollectionObjects)

	// Build filter: match bucket and exclude soft-deleted objects
	filter := bson.M{
		"object.bucket": bucketID,
		"deleted_at":    bson.M{"$exists": false},
	}

	// Add prefix filter if provided
	prefix := strings.TrimSpace(in.GetPrefix())
	if prefix != "" {
		// Filter objects whose names begin with the prefix (case-sensitive)
		filter["object.name"] = bson.M{
			"$regex": "^" + regexp.QuoteMeta(prefix),
		}
	}

	// Fetch one extra document to determine if there is a next page
	limit := int64(pageSize) + 1
	opts := options.Find().
		SetSort(bson.D{{Key: "object.name", Value: 1}}). // Sort by object name
		SetSkip(offset).
		SetLimit(limit)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list objects: %v", err)
	}
	defer cursor.Close(ctx)

	var dbObjects []db.ObjectDocument
	if err := cursor.All(ctx, &dbObjects); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode objects: %v", err)
	}

	hasMore := int32(len(dbObjects)) > pageSize
	if hasMore {
		dbObjects = dbObjects[:pageSize]
	}

	res := &storage.ListObjectsResponse{
		Objects:       make([]*storage.Object, 0, len(dbObjects)),
		Prefixes:      []string{}, // Empty since we're not implementing delimiter
		NextPageToken: "",
	}

	for _, doc := range dbObjects {
		res.Objects = append(res.Objects, doc.Object)
	}

	if hasMore {
		res.NextPageToken = strconv.Itoa(int(offset) + len(res.Objects))
	}

	return res, nil
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

func (e *CloudStorageEmulator) RestoreObject(context.Context, *storage.RestoreObjectRequest) (*storage.Object, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RestoreObject not implemented")
}
func (e *CloudStorageEmulator) CancelResumableWrite(context.Context, *storage.CancelResumableWriteRequest) (*storage.CancelResumableWriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CancelResumableWrite not implemented")
}

func (e *CloudStorageEmulator) BidiReadObject(storage.Storage_BidiReadObjectServer) error {
	return status.Errorf(codes.Unimplemented, "method BidiReadObject not implemented")
}

func (e *CloudStorageEmulator) BidiWriteObject(server storage.Storage_BidiWriteObjectServer) error {
	ctx := server.Context()

	// Receive first message to determine if this is resumable or non-resumable
	firstReq, err := server.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Errorf(codes.InvalidArgument, "no data received")
		}
		return status.Errorf(codes.Internal, "failed to receive first message: %v", err)
	}

	var uploadID string
	var spec *storage.WriteObjectSpec
	var bucketName, objectName string

	// Determine if this is a resumable or non-resumable write
	switch msg := firstReq.FirstMessage.(type) {
	case *storage.BidiWriteObjectRequest_UploadId:
		uploadID = msg.UploadId
		// Load upload metadata to get bucket and object name
		upload, err := db.SelectUploadByID(ctx, e.mongodb, uploadID)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return status.Errorf(codes.NotFound, "upload_id not found: %s", uploadID)
			}
			return status.Errorf(codes.Internal, "failed to get upload: %v", err)
		}
		bucketName = upload.Bucket
		objectName = upload.ObjectName
		spec = upload.Spec
	case *storage.BidiWriteObjectRequest_WriteObjectSpec:
		spec = msg.WriteObjectSpec
		if spec == nil || spec.Resource == nil {
			return status.Errorf(codes.InvalidArgument, "WriteObjectSpec is required for non-resumable writes")
		}
		bucketName = spec.Resource.Bucket
		objectName = spec.Resource.Name
		if bucketName == "" || objectName == "" {
			return status.Errorf(codes.InvalidArgument, "bucket and object name are required")
		}
		// Create upload for tracking
		uploadID, err = db.InsertUpload(ctx, e.mongodb, bucketName, objectName, spec)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create upload: %v", err)
		}
	default:
		return status.Errorf(codes.InvalidArgument, "first message must contain either upload_id or write_object_spec")
	}

	// Validate bucket exists
	_, err = db.SelectBucketByName(ctx, e.mongodb, bucketName)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return status.Errorf(codes.NotFound, "bucket %s not found", bucketName)
		}
		return status.Errorf(codes.Internal, "failed to get bucket: %v", err)
	}

	// Get or create GridFS bucket
	bucketOpts := options.GridFSBucket().SetName("cloudstorageemulator_objects")
	gridFSBucket, err := gridfs.NewBucket(e.mongodb, bucketOpts)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create GridFS bucket: %v", err)
	}

	// Determine write offset
	writeOffset := firstReq.WriteOffset
	if writeOffset < 0 {
		return status.Errorf(codes.InvalidArgument, "write_offset must be non-negative")
	}

	// Get current upload state
	upload, err := db.SelectUploadByID(ctx, e.mongodb, uploadID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get upload state: %v", err)
	}

	// For resumable writes, validate offset matches persisted size
	if writeOffset > 0 && writeOffset != upload.PersistedSize {
		return status.Errorf(codes.InvalidArgument, "write_offset %d does not match persisted_size %d", writeOffset, upload.PersistedSize)
	}

	// Prepare GridFS upload stream
	var uploadStream *gridfs.UploadStream
	var fileID primitive.ObjectID
	filename := fmt.Sprintf("%s/%s", bucketName, objectName)

	if writeOffset == 0 {
		// Starting fresh - create new upload stream
		uploadStream, err = gridFSBucket.OpenUploadStream(filename)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to open GridFS upload stream: %v", err)
		}
		fileID, ok := uploadStream.FileID.(primitive.ObjectID)
		if !ok {
			uploadStream.Close()
			return status.Errorf(codes.Internal, "failed to convert FileID to ObjectID")
		}
		upload.GridFSFileID = fileID
		err = db.UpdateUploadFileID(ctx, e.mongodb, uploadID, fileID)
		if err != nil {
			uploadStream.Close()
			return status.Errorf(codes.Internal, "failed to update upload file ID: %v", err)
		}
	} else {
		// Resume existing upload - open stream with existing file ID
		fileID = upload.GridFSFileID
		if fileID.IsZero() {
			return status.Errorf(codes.InvalidArgument, "cannot resume upload without file ID")
		}
		uploadStream, err = gridFSBucket.OpenUploadStreamWithID(fileID, filename)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resume GridFS upload: %v", err)
		}
		// For resumable uploads, we need to read existing data to validate checksums
		// Note: GridFS doesn't support seeking, so we read the existing file
		downloadStream, err := gridFSBucket.OpenDownloadStream(fileID)
		if err == nil {
			existingData := make([]byte, writeOffset)
			_, err = downloadStream.Read(existingData)
			downloadStream.Close()
			if err != nil && err != io.EOF {
				uploadStream.Close()
				return status.Errorf(codes.Internal, "failed to read existing file data: %v", err)
			}
		}
	}

	defer uploadStream.Close()

	// Process data chunks and write to GridFS
	var totalWritten int64 = writeOffset
	var allData []byte
	var finished bool
	var expectedCrc32c *uint32
	var expectedMd5Hash []byte
	hasFirstMessage := true

	// Load existing data if resuming (for checksum validation)
	if writeOffset > 0 {
		downloadStream, err := gridFSBucket.OpenDownloadStream(fileID)
		if err == nil {
			existingData := make([]byte, writeOffset)
			_, err = downloadStream.Read(existingData)
			downloadStream.Close()
			if err != nil && err != io.EOF {
				return status.Errorf(codes.Internal, "failed to read existing file data: %v", err)
			}
			allData = existingData
		}
	}

	// Process data chunks
	for {
		var req *storage.BidiWriteObjectRequest
		if hasFirstMessage {
			req = firstReq
			hasFirstMessage = false
		} else {
			var recvErr error
			req, recvErr = server.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return status.Errorf(codes.Internal, "failed to receive message: %v", recvErr)
			}
		}

		// Validate write offset
		if req.WriteOffset != totalWritten {
			return status.Errorf(codes.InvalidArgument, "write_offset mismatch: expected %d, got %d", totalWritten, req.WriteOffset)
		}

		// Process data chunk
		if checksummedData := req.GetChecksummedData(); checksummedData != nil {
			data := checksummedData.Content

			// Validate CRC32C if provided (GCS uses CRC32C Castagnoli)
			if checksummedData.Crc32C != nil {
				calculated := crc32.Checksum(data, crc32cTable)
				if calculated != *checksummedData.Crc32C {
					return status.Errorf(codes.InvalidArgument, "CRC32C checksum mismatch")
				}
			}

			// Write to GridFS stream
			n, err := uploadStream.Write(data)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to write to GridFS: %v", err)
			}
			totalWritten += int64(n)
			allData = append(allData, data...)
		}

		// Check for object-level checksums (only in first or last message)
		if req.ObjectChecksums != nil {
			if req.ObjectChecksums.Crc32C != nil {
				expectedCrc32c = req.ObjectChecksums.Crc32C
			}
			if len(req.ObjectChecksums.Md5Hash) > 0 {
				expectedMd5Hash = req.ObjectChecksums.Md5Hash
			}
		}

		// Check if write is finished
		if req.FinishWrite {
			finished = true
			break
		}
	}

	// Update persisted size
	err = db.UpdateUploadPersistedSize(ctx, e.mongodb, uploadID, totalWritten)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to update persisted size: %v", err)
	}

	// If not finished, return persisted size
	if !finished {
		return server.Send(&storage.BidiWriteObjectResponse{
			WriteStatus: &storage.BidiWriteObjectResponse_PersistedSize{
				PersistedSize: totalWritten,
			},
		})
	}

	// Close the upload stream to finalize the file
	err = uploadStream.Close()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to close GridFS upload stream: %v", err)
	}

	// Validate final checksums if provided
	if expectedCrc32c != nil {
		calculated := crc32.Checksum(allData, crc32cTable)
		if calculated != *expectedCrc32c {
			return status.Errorf(codes.InvalidArgument, "object CRC32C checksum mismatch")
		}
	}
	if len(expectedMd5Hash) > 0 {
		hash := md5.Sum(allData)
		if string(hash[:]) != string(expectedMd5Hash) {
			return status.Errorf(codes.InvalidArgument, "object MD5 checksum mismatch")
		}
	}

	// Create object metadata
	now := time.Now()
	object := &storage.Object{
		Name:           objectName,
		Bucket:         bucketName,
		Size:           totalWritten,
		Generation:     time.Now().UnixNano(), // Simple generation based on timestamp
		Metageneration: 1,
		CreateTime:     timestamppb.New(now),
		UpdateTime:     timestamppb.New(now),
		ContentType:    spec.Resource.ContentType,
	}

	// Set checksums in object
	if expectedCrc32c != nil {
		object.Checksums = &storage.ObjectChecksums{
			Crc32C: expectedCrc32c,
		}
	}
	if len(expectedMd5Hash) > 0 {
		if object.Checksums == nil {
			object.Checksums = &storage.ObjectChecksums{}
		}
		object.Checksums.Md5Hash = expectedMd5Hash
	}

	// Store object metadata
	err = db.InsertObject(ctx, e.mongodb, object, upload.GridFSFileID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to store object metadata: %v", err)
	}

	// Mark upload as completed
	err = db.CompleteUpload(ctx, e.mongodb, uploadID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to complete upload: %v", err)
	}

	// Return final object
	return server.Send(&storage.BidiWriteObjectResponse{
		WriteStatus: &storage.BidiWriteObjectResponse_Resource{
			Resource: object,
		},
	})
}

func (e *CloudStorageEmulator) MoveObject(context.Context, *storage.MoveObjectRequest) (*storage.Object, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MoveObject not implemented")
}

// should be in the format projects/PROJECT_ID
// PROJECT_ID should be a valid alphanumeric string or '_' to represent global project alias
// it must start with projects/ and can only be followed by a valid project id format.
func validateProjectParent(parent string) error {
	if !strings.HasPrefix(parent, "projects/") {
		return status.Errorf(codes.InvalidArgument, "invalid project parent: %s", parent)
	}
	projectID := strings.TrimPrefix(parent, "projects/")
	if projectID == "" {
		return status.Errorf(codes.InvalidArgument, "project ID is required")
	}
	if projectID == "_" {
		return nil
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
