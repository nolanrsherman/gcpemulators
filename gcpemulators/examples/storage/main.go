package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/nolanrsherman/gcpemulators/gcpemulators"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	emulator, cleanup, err := gcpemulators.NewCloudStorageEmulator()
	if err != nil {
		log.Fatalf("failed to start cloud storage emulator: %v", err)
	}
	defer cleanup()

	fmt.Println("Cloud storage emulator started on port", emulator.Port)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", emulator.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create grpc client: %v", err)
	}
	storageClient, err := storage.NewGRPCClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("failed to create storage client: %v", err)
	}

	err = storageClient.Bucket("my-bucket-name").Create(context.Background(), "12345", nil)
	if err != nil {
		log.Fatalf("failed to create bucket: %v", err)
	}

	bucketName := storageClient.Bucket("my-bucket-name").BucketName()
	fmt.Println("Bucket created:", bucketName)
}
