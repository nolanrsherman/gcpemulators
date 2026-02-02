package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/storage/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	buckets sync.Map // map[string]*storage.Bucket - concurrency safe
	objects sync.Map // map[string]*storedObject - concurrency safe
	// Embed the unimplemented server to satisfy the interface
	storage.UnimplementedStorageServer
}

func NewCloudStorageEmulator(port int, logger *zap.Logger) *CloudStorageEmulator {
	return &CloudStorageEmulator{
		port:   port,
		logger: logger,
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

// Implement StorageServer interface methods as needed
// GetBucket retrieves bucket metadata and converts it to the protobuf format
func (e *CloudStorageEmulator) GetBucket(ctx context.Context, req *storage.GetBucketRequest) (*storage.Bucket, error) {
	bucketName := req.GetName()

	// Load bucket from sync.Map
	value, exists := e.buckets.Load(bucketName)
	if !exists {
		return nil, fmt.Errorf("bucket %s not found", bucketName)
	}

	bucket, ok := value.(*storage.Bucket)
	if !ok {
		return nil, fmt.Errorf("invalid bucket type stored for %s", bucketName)
	}

	return bucket, nil
}

// CreateBucket creates a new bucket
func (e *CloudStorageEmulator) CreateBucket(ctx context.Context, req *storage.CreateBucketRequest) (*storage.Bucket, error) {
	// Get bucket name from BucketId (required field)
	bucketName := req.GetBucketId()
	if bucketName == "" {
		return nil, fmt.Errorf("bucket_id is required")
	}

	// Check if bucket already exists
	if _, exists := e.buckets.Load(bucketName); exists {
		return nil, fmt.Errorf("bucket %s already exists", bucketName)
	}

	// Convert request to bucket
	bucket := &storage.Bucket{
		Name:       bucketName,
		BucketId:   bucketName,
		CreateTime: timestamppb.Now(),
		UpdateTime: timestamppb.Now(),
	}

	// If a bucket configuration is provided in the request, merge it
	if req.GetBucket() != nil {
		reqBucket := req.GetBucket()
		// Copy fields from request bucket
		if reqBucket.GetLocation() != "" {
			bucket.Location = reqBucket.GetLocation()
		}
		if reqBucket.GetStorageClass() != "" {
			bucket.StorageClass = reqBucket.GetStorageClass()
		}
		if reqBucket.GetVersioning() != nil {
			bucket.Versioning = reqBucket.GetVersioning()
		}
		if reqBucket.GetLabels() != nil {
			bucket.Labels = reqBucket.GetLabels()
		}
		if reqBucket.GetCors() != nil {
			bucket.Cors = reqBucket.GetCors()
		}
		if reqBucket.GetLifecycle() != nil {
			bucket.Lifecycle = reqBucket.GetLifecycle()
		}
		if reqBucket.GetEncryption() != nil {
			bucket.Encryption = reqBucket.GetEncryption()
		}
		if reqBucket.GetRetentionPolicy() != nil {
			bucket.RetentionPolicy = reqBucket.GetRetentionPolicy()
		}
		if reqBucket.GetIamConfig() != nil {
			bucket.IamConfig = reqBucket.GetIamConfig()
		}
		if reqBucket.GetWebsite() != nil {
			bucket.Website = reqBucket.GetWebsite()
		}
		if reqBucket.GetLogging() != nil {
			bucket.Logging = reqBucket.GetLogging()
		}
		if reqBucket.GetBilling() != nil {
			bucket.Billing = reqBucket.GetBilling()
		}
		if reqBucket.GetCustomPlacementConfig() != nil {
			bucket.CustomPlacementConfig = reqBucket.GetCustomPlacementConfig()
		}
		if reqBucket.GetAutoclass() != nil {
			bucket.Autoclass = reqBucket.GetAutoclass()
		}
		if reqBucket.GetRpo() != "" {
			bucket.Rpo = reqBucket.GetRpo()
		}
		bucket.DefaultEventBasedHold = reqBucket.GetDefaultEventBasedHold()
		if reqBucket.GetMetageneration() != 0 {
			bucket.Metageneration = reqBucket.GetMetageneration()
		}
	}

	// Store the bucket in the map
	e.buckets.Store(bucketName, bucket)

	return bucket, nil
}

// DeleteBucket deletes a bucket
func (e *CloudStorageEmulator) DeleteBucket(ctx context.Context, req *storage.DeleteBucketRequest) (*emptypb.Empty, error) {
	// Extract bucket name from the request
	bucketName := req.GetName()
	if bucketName == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	// Parse bucket name if it's in resource format
	// Format: "projects/{project}/buckets/{bucket}" or just "{bucket}"
	// Use LastIndex to get everything after the last "/" (similar to parseBucketName in storage package)
	if sep := strings.LastIndex(bucketName, "/"); sep >= 0 {
		bucketName = bucketName[sep+1:]
	}

	// Load bucket to check if it exists and for metageneration checks
	value, exists := e.buckets.Load(bucketName)
	if !exists {
		return nil, fmt.Errorf("bucket %s not found", bucketName)
	}

	bucket, ok := value.(*storage.Bucket)
	if !ok {
		return nil, fmt.Errorf("invalid bucket type stored for %s", bucketName)
	}

	// Check metageneration conditions if provided
	if req.IfMetagenerationMatch != nil {
		if bucket.GetMetageneration() != *req.IfMetagenerationMatch {
			return nil, fmt.Errorf("bucket metageneration mismatch: expected %d, got %d",
				*req.IfMetagenerationMatch, bucket.GetMetageneration())
		}
	}

	if req.IfMetagenerationNotMatch != nil {
		if bucket.GetMetageneration() == *req.IfMetagenerationNotMatch {
			return nil, fmt.Errorf("bucket metageneration matches: got %d (should not match)",
				bucket.GetMetageneration())
		}
	}

	// Delete the bucket from the map
	e.buckets.Delete(bucketName)

	return &emptypb.Empty{}, nil
}

func (e *CloudStorageEmulator) DeleteObject(ctx context.Context, req *storage.DeleteObjectRequest) (*emptypb.Empty, error) {
	// Extract bucket and object name
	bucketName := req.GetBucket()
	objectName := req.GetObject()

	// For now, we only support deleting by bucket/object (not upload_id)
	if bucketName == "" || objectName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bucket and object name are required")
	}

	// Construct object key
	objectKey := fmt.Sprintf("%s/%s", bucketName, objectName)

	// Load existing object
	value, exists := e.objects.Load(objectKey)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "object %s not found", objectKey)
	}

	storedObj, ok := value.(*storedObject)
	if !ok {
		return nil, status.Errorf(codes.Internal, "invalid object type stored for %s", objectKey)
	}

	object := storedObj.object

	// Check generation conditions
	if req.IfGenerationMatch != nil {
		if object.GetGeneration() != *req.IfGenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object generation mismatch: expected %d, got %d",
				*req.IfGenerationMatch, object.GetGeneration())
		}
	}

	if req.IfGenerationNotMatch != nil {
		if object.GetGeneration() == *req.IfGenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object generation matches: got %d (should not match)",
				object.GetGeneration())
		}
	}

	// Check metageneration conditions
	if req.IfMetagenerationMatch != nil {
		if object.GetMetageneration() != *req.IfMetagenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object metageneration mismatch: expected %d, got %d",
				*req.IfMetagenerationMatch, object.GetMetageneration())
		}
	}

	if req.IfMetagenerationNotMatch != nil {
		if object.GetMetageneration() == *req.IfMetagenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object metageneration matches: got %d (should not match)",
				object.GetMetageneration())
		}
	}

	// Delete the object from the map
	e.objects.Delete(objectKey)

	return &emptypb.Empty{}, nil
}

func (e *CloudStorageEmulator) GetObject(ctx context.Context, req *storage.GetObjectRequest) (*storage.Object, error) {
	// Extract bucket and object name
	bucketName := req.GetBucket()
	objectName := req.GetObject()

	if bucketName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bucket name is required")
	}
	if objectName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "object name is required")
	}

	// Construct object key
	objectKey := fmt.Sprintf("%s/%s", bucketName, objectName)

	// Load existing object
	value, exists := e.objects.Load(objectKey)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "object %s not found", objectKey)
	}

	storedObj, ok := value.(*storedObject)
	if !ok {
		return nil, status.Errorf(codes.Internal, "invalid object type stored for %s", objectKey)
	}

	object := storedObj.object

	// Check generation conditions
	if req.IfGenerationMatch != nil {
		if object.GetGeneration() != *req.IfGenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object generation mismatch: expected %d, got %d",
				*req.IfGenerationMatch, object.GetGeneration())
		}
	}

	if req.IfGenerationNotMatch != nil {
		if object.GetGeneration() == *req.IfGenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object generation matches: got %d (should not match)",
				object.GetGeneration())
		}
	}

	// Check metageneration conditions
	if req.IfMetagenerationMatch != nil {
		if object.GetMetageneration() != *req.IfMetagenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object metageneration mismatch: expected %d, got %d",
				*req.IfMetagenerationMatch, object.GetMetageneration())
		}
	}

	if req.IfMetagenerationNotMatch != nil {
		if object.GetMetageneration() == *req.IfMetagenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object metageneration matches: got %d (should not match)",
				object.GetMetageneration())
		}
	}

	return object, nil
}

func (e *CloudStorageEmulator) ReadObject(req *storage.ReadObjectRequest, stream storage.Storage_ReadObjectServer) error {
	// Extract bucket and object name
	bucketName := req.GetBucket()
	objectName := req.GetObject()

	if bucketName == "" {
		return status.Errorf(codes.InvalidArgument, "bucket name is required")
	}
	if objectName == "" {
		return status.Errorf(codes.InvalidArgument, "object name is required")
	}

	// Construct object key
	objectKey := fmt.Sprintf("%s/%s", bucketName, objectName)

	// Load existing object
	value, exists := e.objects.Load(objectKey)
	if !exists {
		return status.Errorf(codes.NotFound, "object %s not found", objectKey)
	}

	storedObj, ok := value.(*storedObject)
	if !ok {
		return status.Errorf(codes.Internal, "invalid object type stored for %s", objectKey)
	}

	object := storedObj.object
	data := storedObj.data

	// Check generation conditions
	if req.IfGenerationMatch != nil {
		if object.GetGeneration() != *req.IfGenerationMatch {
			return status.Errorf(codes.FailedPrecondition,
				"object generation mismatch: expected %d, got %d",
				*req.IfGenerationMatch, object.GetGeneration())
		}
	}

	if req.IfGenerationNotMatch != nil {
		if object.GetGeneration() == *req.IfGenerationNotMatch {
			return status.Errorf(codes.FailedPrecondition,
				"object generation matches: got %d (should not match)",
				object.GetGeneration())
		}
	}

	// Check metageneration conditions
	if req.IfMetagenerationMatch != nil {
		if object.GetMetageneration() != *req.IfMetagenerationMatch {
			return status.Errorf(codes.FailedPrecondition,
				"object metageneration mismatch: expected %d, got %d",
				*req.IfMetagenerationMatch, object.GetMetageneration())
		}
	}

	if req.IfMetagenerationNotMatch != nil {
		if object.GetMetageneration() == *req.IfMetagenerationNotMatch {
			return status.Errorf(codes.FailedPrecondition,
				"object metageneration matches: got %d (should not match)",
				object.GetMetageneration())
		}
	}

	// Calculate read offset (handle negative offsets)
	dataSize := int64(len(data))
	readOffset := req.GetReadOffset()
	if readOffset < 0 {
		// Negative offset means from the end
		readOffset = dataSize + readOffset
		if readOffset < 0 {
			readOffset = 0
		}
	}
	if readOffset > dataSize {
		readOffset = dataSize
	}

	// Calculate read limit
	readLimit := req.GetReadLimit()
	if readLimit < 0 {
		return status.Errorf(codes.InvalidArgument, "read_limit must be non-negative")
	}
	if readLimit == 0 {
		// No limit, read to end
		readLimit = dataSize - readOffset
	}

	// Calculate actual end position
	actualEnd := readOffset + readLimit
	if actualEnd > dataSize {
		actualEnd = dataSize
	}
	actualLimit := actualEnd - readOffset

	// Extract the data slice to send
	dataToSend := data[readOffset:actualEnd]

	// Send first response with metadata and ContentRange (if offset/limit specified)
	firstResponse := &storage.ReadObjectResponse{
		Metadata: object,
	}

	// Include ContentRange if read_offset or read_limit was specified
	if req.GetReadOffset() != 0 || req.GetReadLimit() != 0 {
		firstResponse.ContentRange = &storage.ContentRange{
			Start:          readOffset,
			End:            actualEnd - 1, // End is inclusive
			CompleteLength: dataSize,
		}
	}

	if err := stream.Send(firstResponse); err != nil {
		return status.Errorf(codes.Internal, "failed to send first response: %v", err)
	}

	// Stream data in chunks (1MB chunks)
	const chunkSize = 1024 * 1024 // 1MB
	for offset := int64(0); offset < actualLimit; offset += chunkSize {
		chunkEnd := offset + chunkSize
		if chunkEnd > actualLimit {
			chunkEnd = actualLimit
		}

		chunk := dataToSend[offset:chunkEnd]
		response := &storage.ReadObjectResponse{
			ChecksummedData: &storage.ChecksummedData{
				Content: chunk,
			},
		}

		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Internal, "failed to send data chunk: %v", err)
		}
	}

	return nil
}

func (e *CloudStorageEmulator) UpdateObject(ctx context.Context, req *storage.UpdateObjectRequest) (*storage.Object, error) {
	// Validate request
	if req.GetObject() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "object is required")
	}

	object := req.GetObject()
	bucketName := object.GetBucket()
	objectName := object.GetName()

	if bucketName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bucket name is required")
	}
	if objectName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "object name is required")
	}

	// Construct object key
	objectKey := fmt.Sprintf("%s/%s", bucketName, objectName)

	// Load existing object
	value, exists := e.objects.Load(objectKey)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "object %s not found", objectKey)
	}

	storedObj, ok := value.(*storedObject)
	if !ok {
		return nil, status.Errorf(codes.Internal, "invalid object type stored for %s", objectKey)
	}

	existingObject := storedObj.object

	// Check generation conditions
	if req.IfGenerationMatch != nil {
		if existingObject.GetGeneration() != *req.IfGenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object generation mismatch: expected %d, got %d",
				*req.IfGenerationMatch, existingObject.GetGeneration())
		}
	}

	if req.IfGenerationNotMatch != nil {
		if existingObject.GetGeneration() == *req.IfGenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object generation matches: got %d (should not match)",
				existingObject.GetGeneration())
		}
	}

	// Check metageneration conditions
	if req.IfMetagenerationMatch != nil {
		if existingObject.GetMetageneration() != *req.IfMetagenerationMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object metageneration mismatch: expected %d, got %d",
				*req.IfMetagenerationMatch, existingObject.GetMetageneration())
		}
	}

	if req.IfMetagenerationNotMatch != nil {
		if existingObject.GetMetageneration() == *req.IfMetagenerationNotMatch {
			return nil, status.Errorf(codes.FailedPrecondition,
				"object metageneration matches: got %d (should not match)",
				existingObject.GetMetageneration())
		}
	}

	// Validate update mask
	updateMask := req.GetUpdateMask()
	if updateMask == nil || len(updateMask.GetPaths()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "update_mask is required")
	}

	// Create a copy of the existing object to update
	updatedObject := proto.Clone(existingObject).(*storage.Object)

	// Apply updates based on the field mask
	for _, path := range updateMask.GetPaths() {
		switch path {
		case "content_type":
			updatedObject.ContentType = object.GetContentType()
		case "content_encoding":
			updatedObject.ContentEncoding = object.GetContentEncoding()
		case "content_disposition":
			updatedObject.ContentDisposition = object.GetContentDisposition()
		case "cache_control":
			updatedObject.CacheControl = object.GetCacheControl()
		case "metadata":
			updatedObject.Metadata = object.GetMetadata()
		case "storage_class":
			updatedObject.StorageClass = object.GetStorageClass()
		case "custom_time":
			updatedObject.CustomTime = object.GetCustomTime()
		case "event_based_hold":
			// EventBasedHold is *bool, so we need to create a pointer
			if object.EventBasedHold != nil {
				val := *object.EventBasedHold
				updatedObject.EventBasedHold = &val
			} else {
				updatedObject.EventBasedHold = nil
			}
		case "temporary_hold":
			updatedObject.TemporaryHold = object.GetTemporaryHold()
		case "acl":
			updatedObject.Acl = object.GetAcl()
		case "update_time":
			// Update time is automatically set below
		case "*":
			// Update all fields (not recommended but supported)
			if object.GetContentType() != "" {
				updatedObject.ContentType = object.GetContentType()
			}
			if object.GetContentEncoding() != "" {
				updatedObject.ContentEncoding = object.GetContentEncoding()
			}
			if object.GetContentDisposition() != "" {
				updatedObject.ContentDisposition = object.GetContentDisposition()
			}
			if object.GetCacheControl() != "" {
				updatedObject.CacheControl = object.GetCacheControl()
			}
			if object.GetMetadata() != nil {
				updatedObject.Metadata = object.GetMetadata()
			}
			if object.GetStorageClass() != "" {
				updatedObject.StorageClass = object.GetStorageClass()
			}
			if object.GetCustomTime() != nil {
				updatedObject.CustomTime = object.GetCustomTime()
			}
			// EventBasedHold is *bool, so we need to create a pointer
			if object.EventBasedHold != nil {
				val := *object.EventBasedHold
				updatedObject.EventBasedHold = &val
			} else {
				updatedObject.EventBasedHold = nil
			}
			updatedObject.TemporaryHold = object.GetTemporaryHold()
			if object.GetAcl() != nil {
				updatedObject.Acl = object.GetAcl()
			}
		}
	}

	// Update metadata fields
	updatedObject.UpdateTime = timestamppb.Now()
	updatedObject.Metageneration = existingObject.GetMetageneration() + 1

	// Store the updated object (data remains unchanged)
	updatedStoredObj := &storedObject{
		object: updatedObject,
		data:   storedObj.data, // Keep existing data
	}
	e.objects.Store(objectKey, updatedStoredObj)

	return updatedObject, nil
}
func (e *CloudStorageEmulator) WriteObject(stream storage.Storage_WriteObjectServer) error {
	var (
		objectSpec   *storage.WriteObjectSpec
		objectName   string
		bucketName   string
		dataBuffer   bytes.Buffer
		writeOffset  int64
		firstMessage = true
	)

	// Receive streaming requests
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Client closed the stream
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive request: %v", err)
		}

		// Handle first message - should contain WriteObjectSpec
		if firstMessage {
			firstMessage = false
			spec := req.GetWriteObjectSpec()
			if spec == nil {
				return status.Errorf(codes.InvalidArgument, "first message must contain WriteObjectSpec")
			}
			objectSpec = spec

			// Extract bucket and object name from the resource
			if spec.GetResource() == nil {
				return status.Errorf(codes.InvalidArgument, "WriteObjectSpec must contain resource")
			}
			resource := spec.GetResource()
			objectName = resource.GetName()
			bucketName = resource.GetBucket()

			if objectName == "" {
				return status.Errorf(codes.InvalidArgument, "object name is required")
			}
			if bucketName == "" {
				return status.Errorf(codes.InvalidArgument, "bucket name is required")
			}

			// Verify bucket exists
			if _, exists := e.buckets.Load(bucketName); !exists {
				return status.Errorf(codes.NotFound, "bucket %s not found", bucketName)
			}

			// Initialize write offset
			writeOffset = req.GetWriteOffset()
			if writeOffset < 0 {
				return status.Errorf(codes.InvalidArgument, "write_offset must be non-negative")
			}
		}

		// Handle data chunks
		if checksummedData := req.GetChecksummedData(); checksummedData != nil {
			data := checksummedData.GetContent()
			expectedOffset := writeOffset + int64(dataBuffer.Len())

			// Validate write offset
			if req.GetWriteOffset() != expectedOffset {
				return status.Errorf(codes.InvalidArgument,
					"write_offset mismatch: expected %d, got %d", expectedOffset, req.GetWriteOffset())
			}

			// Append data to buffer
			if _, err := dataBuffer.Write(data); err != nil {
				return status.Errorf(codes.Internal, "failed to write data: %v", err)
			}
		}

		// Check if this is the final message
		if req.GetFinishWrite() {
			// Create the object
			object := &storage.Object{
				Name:           objectName,
				Bucket:         bucketName,
				Size:           int64(dataBuffer.Len()),
				CreateTime:     timestamppb.Now(),
				UpdateTime:     timestamppb.Now(),
				Generation:     1, // Start with generation 1
				Metageneration: 1,
			}

			// Copy metadata from spec if provided
			if objectSpec.GetResource() != nil {
				resource := objectSpec.GetResource()
				if resource.GetContentType() != "" {
					object.ContentType = resource.GetContentType()
				}
				if resource.GetContentEncoding() != "" {
					object.ContentEncoding = resource.GetContentEncoding()
				}
				if resource.GetContentDisposition() != "" {
					object.ContentDisposition = resource.GetContentDisposition()
				}
				if resource.GetCacheControl() != "" {
					object.CacheControl = resource.GetCacheControl()
				}
				if resource.GetMetadata() != nil {
					object.Metadata = resource.GetMetadata()
				}
				if resource.GetStorageClass() != "" {
					object.StorageClass = resource.GetStorageClass()
				}
			}

			// Store object with its data
			objectKey := fmt.Sprintf("%s/%s", bucketName, objectName)
			storedObj := &storedObject{
				object: object,
				data:   dataBuffer.Bytes(),
			}
			e.objects.Store(objectKey, storedObj)

			// Send response with the created object
			response := &storage.WriteObjectResponse{
				WriteStatus: &storage.WriteObjectResponse_Resource{
					Resource: object,
				},
			}

			return stream.SendAndClose(response)
		}
	}

	// If we get here, finish_write was never set
	return status.Errorf(codes.InvalidArgument, "finish_write must be set in the final message")
}
func (e *CloudStorageEmulator) ListObjects(ctx context.Context, req *storage.ListObjectsRequest) (*storage.ListObjectsResponse, error) {
	// Extract bucket name from Parent (format: "buckets/{bucket}" or just "{bucket}")
	parent := req.GetParent()
	if parent == "" {
		return nil, status.Errorf(codes.InvalidArgument, "parent is required")
	}

	bucketName := parent
	if strings.HasPrefix(parent, "buckets/") {
		bucketName = strings.TrimPrefix(parent, "buckets/")
	}

	// Verify bucket exists
	if _, exists := e.buckets.Load(bucketName); !exists {
		return nil, status.Errorf(codes.NotFound, "bucket %s not found", bucketName)
	}

	// Collect all objects for this bucket
	var allObjects []*storage.Object
	var allObjectKeys []string

	e.objects.Range(func(key, value interface{}) bool {
		objectKey := key.(string)
		// Check if object belongs to this bucket
		if !strings.HasPrefix(objectKey, bucketName+"/") {
			return true // Continue iteration
		}

		storedObj, ok := value.(*storedObject)
		if !ok {
			return true // Continue iteration
		}

		// Extract object name (everything after "bucket/")
		objectName := strings.TrimPrefix(objectKey, bucketName+"/")

		// Apply prefix filter
		prefix := req.GetPrefix()
		if prefix != "" && !strings.HasPrefix(objectName, prefix) {
			return true // Continue iteration
		}

		// Apply lexicographic range filters
		lexStart := req.GetLexicographicStart()
		if lexStart != "" && objectName < lexStart {
			return true // Continue iteration
		}

		lexEnd := req.GetLexicographicEnd()
		if lexEnd != "" && objectName >= lexEnd {
			return true // Continue iteration
		}

		allObjects = append(allObjects, storedObj.object)
		allObjectKeys = append(allObjectKeys, objectName)
		return true // Continue iteration
	})

	// Sort objects by name (lexicographically)
	// We'll sort the keys and reorder objects accordingly
	for i := 0; i < len(allObjectKeys); i++ {
		for j := i + 1; j < len(allObjectKeys); j++ {
			if allObjectKeys[i] > allObjectKeys[j] {
				allObjectKeys[i], allObjectKeys[j] = allObjectKeys[j], allObjectKeys[i]
				allObjects[i], allObjects[j] = allObjects[j], allObjects[i]
			}
		}
	}

	// Handle delimiter (directory-like listing)
	delimiter := req.GetDelimiter()
	var objects []*storage.Object
	var prefixes []string
	prefixSet := make(map[string]bool)

	if delimiter != "" {
		for i, objectName := range allObjectKeys {
			// Check if object name contains delimiter after prefix
			prefix := req.GetPrefix()
			relativeName := objectName
			if prefix != "" {
				if !strings.HasPrefix(objectName, prefix) {
					continue
				}
				relativeName = strings.TrimPrefix(objectName, prefix)
			}

			delimiterIndex := strings.Index(relativeName, delimiter)
			if delimiterIndex >= 0 {
				// This is a "directory" - add to prefixes
				prefixPath := prefix + relativeName[:delimiterIndex+len(delimiter)]
				if !prefixSet[prefixPath] {
					prefixes = append(prefixes, prefixPath)
					prefixSet[prefixPath] = true
				}

				// Include object in items if it ends with exactly one delimiter and IncludeTrailingDelimiter is true
				if req.GetIncludeTrailingDelimiter() && relativeName == delimiter {
					objects = append(objects, allObjects[i])
				}
			} else {
				// Regular object - add to items
				objects = append(objects, allObjects[i])
			}
		}
	} else {
		// No delimiter - return all objects
		objects = allObjects
	}

	// Handle pagination
	pageSize := req.GetPageSize()
	if pageSize <= 0 {
		pageSize = 1000 // Default page size
	}
	if pageSize > 1000 {
		pageSize = 1000 // Max page size
	}

	pageToken := req.GetPageToken()
	startIndex := 0

	// Simple pagination: use page token as index offset
	if pageToken != "" {
		if parsedIndex, err := strconv.Atoi(pageToken); err == nil {
			startIndex = parsedIndex
		}
	}

	// Calculate end index
	endIndex := startIndex + int(pageSize)
	if endIndex > len(objects) {
		endIndex = len(objects)
	}

	// Get page of objects
	var pagedObjects []*storage.Object
	if startIndex < len(objects) {
		pagedObjects = objects[startIndex:endIndex]
	} else {
		pagedObjects = []*storage.Object{} // Empty slice if startIndex is beyond bounds
	}

	// Determine next page token
	var nextPageToken string
	if endIndex < len(objects) {
		nextPageToken = strconv.Itoa(endIndex)
	}

	// Sort prefixes
	for i := 0; i < len(prefixes); i++ {
		for j := i + 1; j < len(prefixes); j++ {
			if prefixes[i] > prefixes[j] {
				prefixes[i], prefixes[j] = prefixes[j], prefixes[i]
			}
		}
	}

	return &storage.ListObjectsResponse{
		Objects:       pagedObjects,
		Prefixes:      prefixes,
		NextPageToken: nextPageToken,
	}, nil
}
