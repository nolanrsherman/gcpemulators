package main

import (
	"context"
	"fmt"
	"log"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nolanrsherman/gcpemulators/gcpemulators"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	emulator, cleanup, err := gcpemulators.NewCloudTaskEmulator()
	if err != nil {
		log.Fatalf("failed to start cloud tasks emulator: %v", err)
	}
	defer cleanup()

	fmt.Println("Cloud tasks emulator started on port", emulator.Port)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", emulator.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create grpc client: %v", err)
	}
	cloudtasksClient, err := cloudtasks.NewClient(context.Background(), option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("failed to create cloud tasks client: %v", err)
	}

	queue, err := cloudtasksClient.CreateQueue(context.Background(), &cloudtaskspb.CreateQueueRequest{
		Parent: "projects/test-project/locations/us-central1",
		Queue: &cloudtaskspb.Queue{
			Name: "projects/test-project/locations/us-central1/queues/test-queue",
		},
	})
	if err != nil {
		log.Fatalf("failed to create queue: %v", err)
	}

	fmt.Println("Queue created:", queue.Name)
}
