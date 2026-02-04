package cloudstorageemulator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nolanrsherman/gcpemulators/cloudstorageemulator/db"
	storage "github.com/nolanrsherman/gcpemulators/cloudstorageemulator/storagepb"
	"github.com/nolanrsherman/gcpemulators/internal/testcommon"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var testMongoURI = "mongodb://localhost:27017/?directConnection=true"

func FindOpenPort(t *testing.T) int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to find open port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
func TestCloudStorageEmulatorStartAndStop(t *testing.T) {
	port := FindOpenPort(t)
	emulator := NewCloudStorageEmulator(port, zap.NewNop(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := emulator.Start(ctx)
	require.NoError(t, err)
}

func WhenTheCloudStorageEmulatorIsRunning(t *testing.T) (*CloudStorageEmulator, func(t *testing.T)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	mongoDB, cleanup := db.NewTestDatabase(ctx, t, testMongoURI)
	port := FindOpenPort(t)
	emulator := NewCloudStorageEmulator(port, zap.NewNop(), mongoDB)

	errgroup, errgroupCtx := errgroup.WithContext(ctx)
	errgroup.Go(func() error {
		err := emulator.Start(errgroupCtx)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	return emulator, func(t *testing.T) {
		cleanup(t)
		cancel()
		require.NoError(t, errgroup.Wait())
	}
}

func WhenThereIsAGrpcStorageClient(t *testing.T, port int) (storage.StorageClient, func(t *testing.T)) {
	t.Helper()
	storageGrpcConn, err := grpc.NewClient("localhost:"+strconv.Itoa(port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	storageClient := storage.NewStorageClient(storageGrpcConn)
	return storageClient, func(t *testing.T) {
		err := storageGrpcConn.Close()
		require.NoError(t, err)
	}
}

func TestValidationHelpers(t *testing.T) {
	t.Run("should validate project parent", func(t *testing.T) {
		testCases := []struct {
			name          string
			parent        string
			expectedError bool
		}{
			{name: "valid project parent", parent: "projects/test-project", expectedError: false},
			{name: "empty project parent", parent: "", expectedError: true},
			{name: "project parent without projects/", parent: "test-project", expectedError: true},
			{name: "project parent with invalid project ID", parent: "projects/test-project-123!", expectedError: true},
			{name: "project parent ending with /", parent: "projects/test-project/", expectedError: true},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				err := validateProjectParent(testCase.parent)
				if testCase.expectedError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
	t.Run("should validate bucket id", func(t *testing.T) {
		testCases := []struct {
			name          string
			bucketID      string
			expectedError bool
		}{
			{name: "valid bucket id simple", bucketID: "test-bucket", expectedError: false},
			{name: "valid bucket id with numbers", bucketID: "bucket123", expectedError: false},
			{name: "empty bucket id", bucketID: "", expectedError: true},
			{name: "bucket id with invalid character", bucketID: "test_bucket", expectedError: true},
			{name: "bucket id with space", bucketID: "test bucket", expectedError: true},
			{name: "bucket id with space", bucketID: "test-bucket/", expectedError: true},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := validateBucketId(tc.bucketID)
				if tc.expectedError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
	t.Run("should validate bucket name", func(t *testing.T) {
		testCases := []struct {
			name          string
			bucketName    string
			expectedError bool
		}{
			{
				name:          "valid bucket name",
				bucketName:    "projects/test-project/buckets/test-bucket",
				expectedError: false,
			},
			{
				name:          "missing projects prefix",
				bucketName:    "test-project/buckets/test-bucket",
				expectedError: true,
			},
			{
				name:          "missing buckets segment",
				bucketName:    "projects/test-project/test-bucket",
				expectedError: true,
			},
			{
				name:          "invalid bucket id part",
				bucketName:    "projects/test-project/buckets/test_bucket",
				expectedError: true,
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := validateBucketName(tc.bucketName)
				if tc.expectedError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	})
}

func TestCloudStorageEmulatorBucket(t *testing.T) {
	emulator, stopEmulator := WhenTheCloudStorageEmulatorIsRunning(t)
	defer stopEmulator(t)

	storageClient, closeConn := WhenThereIsAGrpcStorageClient(t, emulator.port)
	defer closeConn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	projectID := "test-project"
	bucketAId := "test-bucket-A"
	bucketA, err := storageClient.CreateBucket(ctx, &storage.CreateBucketRequest{
		Parent:   "projects/" + projectID,
		BucketId: bucketAId,
		Bucket:   &storage.Bucket{},
	})
	require.NoError(t, err)
	require.NotNil(t, bucketA)

	bucketBId := "test-bucket-B"
	bucketB, err := storageClient.CreateBucket(ctx, &storage.CreateBucketRequest{
		Parent:   "projects/" + projectID,
		BucketId: bucketBId,
		Bucket:   &storage.Bucket{},
	})
	require.NoError(t, err)
	require.NotNil(t, bucketB)

	// should be able to get a bucket
	t.Run("should be able to get a bucket", func(t *testing.T) {
		actualBucketA, err := storageClient.GetBucket(ctx, &storage.GetBucketRequest{
			Name: "projects/" + projectID + "/buckets/" + bucketAId,
		})
		require.NoError(t, err)
		require.NotNil(t, actualBucketA)
		expectedBucketA := &storage.Bucket{
			Name:         fmt.Sprintf("projects/%s/buckets/%s", projectID, bucketAId),
			Project:      projectID,
			BucketId:     bucketAId,
			Location:     "us-central1",
			StorageClass: "STANDARD",
			CreateTime:   timestamppb.New(time.Now()),
			UpdateTime:   timestamppb.New(time.Now()),
		}
		testcommon.MustBeIdentical(t, expectedBucketA, actualBucketA, protocmp.Transform(), protocmp.IgnoreFields(&storage.Bucket{}, "create_time", "update_time"))
		require.WithinDuration(t, expectedBucketA.CreateTime.AsTime(), actualBucketA.CreateTime.AsTime(), 1*time.Second)
		require.WithinDuration(t, expectedBucketA.UpdateTime.AsTime(), actualBucketA.UpdateTime.AsTime(), 1*time.Second)

		actualBucketB, err := storageClient.GetBucket(ctx, &storage.GetBucketRequest{
			Name: "projects/" + projectID + "/buckets/" + bucketBId,
		})
		require.NoError(t, err)
		require.NotNil(t, actualBucketB)
		expectedBucketB := &storage.Bucket{
			Name:         fmt.Sprintf("projects/%s/buckets/%s", projectID, bucketBId),
			Project:      projectID,
			BucketId:     bucketBId,
			Location:     "us-central1",
			StorageClass: "STANDARD",
			CreateTime:   timestamppb.New(time.Now()),
			UpdateTime:   timestamppb.New(time.Now()),
		}
		testcommon.MustBeIdentical(t, expectedBucketB, actualBucketB, protocmp.Transform(), protocmp.IgnoreFields(&storage.Bucket{}, "create_time", "update_time"))
		require.WithinDuration(t, expectedBucketB.CreateTime.AsTime(), actualBucketB.CreateTime.AsTime(), 1*time.Second)
		require.WithinDuration(t, expectedBucketB.UpdateTime.AsTime(), actualBucketB.UpdateTime.AsTime(), 1*time.Second)
	})
	t.Run("should be able to list buckets", func(t *testing.T) {
		actualBuckets, err := storageClient.ListBuckets(ctx, &storage.ListBucketsRequest{
			Parent: "projects/" + projectID,
		})
		require.NoError(t, err)
		require.NotNil(t, actualBuckets)
		require.Len(t, actualBuckets.Buckets, 2)

		bucketNames := make([]string, 0, len(actualBuckets.Buckets))
		for _, bucket := range actualBuckets.Buckets {
			bucketNames = append(bucketNames, bucket.Name)
		}
		require.Contains(t, bucketNames, bucketA.Name)
		require.Contains(t, bucketNames, bucketB.Name)
	})
	// should be able to delete a bucket
	t.Run("should be able to delete a bucket", func(t *testing.T) {
		_, err := storageClient.DeleteBucket(ctx, &storage.DeleteBucketRequest{
			Name: bucketA.Name,
		})
		require.NoError(t, err)

		actualBuckets, err := storageClient.ListBuckets(ctx, &storage.ListBucketsRequest{
			Parent: "projects/" + projectID,
		})
		require.NoError(t, err)
		require.NotNil(t, actualBuckets)
		require.Len(t, actualBuckets.Buckets, 1)
		require.Contains(t, actualBuckets.Buckets[0].Name, bucketB.Name)
	})
}

func TestCloudStorageEmulatorObject(t *testing.T) {
	emulator, stopEmulator := WhenTheCloudStorageEmulatorIsRunning(t)
	defer stopEmulator(t)

	storageClient, closeConn := WhenThereIsAGrpcStorageClient(t, emulator.port)
	defer closeConn(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	projectID := "test-project"
	bucketBId := "test-bucket"
	bucketA, err := storageClient.CreateBucket(ctx, &storage.CreateBucketRequest{
		Parent:   "projects/" + projectID,
		BucketId: bucketBId,
		Bucket:   &storage.Bucket{},
	})
	require.NoError(t, err)
	require.NotNil(t, bucketA)

	t.Run("should be able to upload an object with non-resumable write", func(t *testing.T) {
		objectName := "non-resumable-test-object"
		testData := []byte("test-data")

		writeClient, err := storageClient.WriteObject(ctx)
		require.NoError(t, err)

		// First message: WriteObjectSpec with object metadata
		err = writeClient.Send(&storage.WriteObjectRequest{
			FirstMessage: &storage.WriteObjectRequest_WriteObjectSpec{
				WriteObjectSpec: &storage.WriteObjectSpec{
					Resource: &storage.Object{
						Bucket: bucketA.BucketId,
						Name:   objectName,
					},
				},
			},
			WriteOffset: 0,
			Data: &storage.WriteObjectRequest_ChecksummedData{
				ChecksummedData: &storage.ChecksummedData{
					Content: testData,
				},
			},
			FinishWrite: true,
		})
		require.NoError(t, err)

		// Close and receive response
		response, err := writeClient.CloseAndRecv()
		require.NoError(t, err)
		require.NotNil(t, response)

		// Verify response contains the object
		resource := response.GetResource()
		require.NotNil(t, resource)
		require.Equal(t, objectName, resource.Name)
		require.Equal(t, bucketA.BucketId, resource.Bucket)
		require.Equal(t, int64(len(testData)), resource.Size)
		require.NotZero(t, resource.Generation)
		require.NotNil(t, resource.CreateTime)
		require.NotNil(t, resource.UpdateTime)

		t.Run("should be able to get an object that was uploaded with non-resumable write", func(t *testing.T) {

			object, err := storageClient.GetObject(ctx, &storage.GetObjectRequest{
				Bucket: bucketA.BucketId,
				Object: objectName,
			})
			require.NoError(t, err)
			require.NotNil(t, object)
			require.Equal(t, objectName, object.Name)
			require.Equal(t, bucketA.BucketId, object.Bucket)
		})

		t.Run("should be able to read an object that was uploaded with non-resumable write", func(t *testing.T) {
			readClient, err := storageClient.ReadObject(ctx, &storage.ReadObjectRequest{
				Bucket: bucketA.BucketId,
				Object: objectName,
			})
			require.NoError(t, err)
			data := make([]byte, 0, len(testData))
			for {
				resp, err := readClient.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				require.NotNil(t, resp)

				// Append data from ChecksummedData if present
				if resp.ChecksummedData != nil && len(resp.ChecksummedData.Content) > 0 {
					data = append(data, resp.ChecksummedData.Content...)
				}
			}
			require.Equal(t, testData, data)
		})

		t.Run("should be able to list objects created with non-resumable write", func(t *testing.T) {
			objects, err := storageClient.ListObjects(ctx, &storage.ListObjectsRequest{
				Parent: bucketA.Name,
				Prefix: objectName,
			})
			require.NoError(t, err)
			require.NotNil(t, objects)
			require.Len(t, objects.Objects, 1)
			require.Equal(t, objectName, objects.Objects[0].Name)
			require.Equal(t, bucketA.BucketId, objects.Objects[0].Bucket)
		})

		t.Run("should be able to delete an object that was uploaded with non-resumable write", func(t *testing.T) {

			_, err := storageClient.DeleteObject(ctx, &storage.DeleteObjectRequest{
				Bucket: bucketA.BucketId,
				Object: objectName,
			})
			require.NoError(t, err)

			objects, err := storageClient.ListObjects(ctx, &storage.ListObjectsRequest{
				Parent: bucketA.Name,
				Prefix: objectName,
			})
			require.NoError(t, err)
			require.NotNil(t, objects)
			require.Len(t, objects.Objects, 0)

			_, err = storageClient.GetObject(ctx, &storage.GetObjectRequest{
				Bucket: bucketA.BucketId,
				Object: objectName,
			})
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.NotFound, st.Code())
		})
	})

}
