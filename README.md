# gcpemulators

A collection of GCP emulators designed to be native to Go and easy to integrate into your test environment.

# About

`gcpemulators` provides Go-native emulators for select GCP services that run as local subprocesses, exposing gRPC endpoints compatible with the official Go client libraries. You can start emulators either via a CLI or via a Go package that manages their lifecycle for your tests.

This is a new project, and I'd welcome contributions! Whether you want to 
add support for additional GCP services, improve existing emulators, fix 
bugs, or enhance documentation, your help is appreciated.

# Challenges

One great thing about GCP services is that most of the APIs have protocol buffers, which makes it easy to handle requests from the client libraries. However, in some cases (like Cloud Storage), the Go client library (`cloud.google.com/go/storage`) uses internal protobufs (`cloud.google.com/go/storage/internal/apiv2/storagepb`) that differ from the public ones for the same service. Using both at once can cause conflicting protobuf namespace registrations and runtime panics.

I first tried solving this by running emulators in Docker containers, but that introduced extra complexity, especially around networking when emulators (like Cloud Tasks) need to call back into services running on your machine. Instead, this project focuses on a Go package that starts emulators as separate subprocesses via the CLI, avoiding protobuf conflicts while keeping local networking simple.

# How to Contribute

Since this project is new, there is not an established structure here yet. Just open up a Pull Request about a service you want to emulate or any bugs/features you want to address, and let's chat.


# Supported Services

| Service       | Status      |   # fn   | gRPC support | HTTP support |
|--------------|--------------|----------|--------------|--------------|
| apigateway   | planned      |    0     | TBD          | TBD          |
| appengine    | planned      |    0     | TBD          | TBD          |
| auth         | planned      |    0     | TBD          | TBD          |
| bigquery     | planned      |    0     | TBD          | TBD          |
| bigtable     | planned      |    0     | TBD          | TBD          |
| cloudsqlconn | planned      |    0     | TBD          | TBD          |
| cloudtasks   | in progress  |  13/16   |  ✅          | TBD          |
| filestore    | planned      |    0     | TBD          | TBD          |
| firestore    | planned      |    0     | TBD          | TBD          |
| functions    | planned      |    0     | TBD          | TBD          |
| pubsub       | planned      |    0     | TBD          | TBD          |
| scheduler    | planned      |    0     | TBD          | TBD          |
| secretmanager| planned      |    0     | TBD          | TBD          |
| storage      | in progress  |  10/24   |  ✅          | TBD          |
| workflows    | planned      |    0     | TBD          | TBD          |

# How to Use

## With CLI

### 1. Install

Install the gcpemulators cli with go
```sh
go install github.com/nolanrsherman/gcpemulators/cmd/gcpemulators@latest
```

### 2. MongoDB Database

The emulators require a MongoDB database. The easiest way to get started is to just host one locally.

Example of starting mongo with docker run.
```sh
docker run  \
  -p 27017:27017 \
  --name mongo-gcpemulators \
  mongo:7.0
```

There are many ways to host MongoDB. `gcpemulators` accepts a standard MongoDB connection string, so you can specify any MongoDB instance you want (local or remote).

### 3. Run

### (Option 1). CLI

After installing the CLI and starting MongoDB, you can start individual emulators from the command line.

See the help menu for more info:
```sh
gcpemulators help
```

Start the **Cloud Tasks** emulator:

```sh
gcpemulators cloudtasks \
  --port 6352 \
  --mongo-uri "mongodb://localhost:27017/?directConnection=true" \
  --db-name "cloudtaskemulator"
```

Start the **Cloud Storage** emulator:

```sh
gcpemulators storage \
  --port 6351 \
  --mongo-uri "mongodb://localhost:27017/?directConnection=true" \
  --db-name "cloudstorageemulator"
```

### (Option 2). `gcpemulators` Go package

Perhaps more useful and convenient for testing is the `gcpemulators` Go package. It can start emulators in-process for your tests and return the ports and clients you need to talk to them.

Start the **Cloud Tasks** emulator:

```go
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

```

Start the **Cloud Storage** emulator:
```go
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
```

## With Docker

### Docker Run

The published Docker image bundles the `gcpemulators` binary and a MongoDB instance in a single container.
You run it by choosing which emulator to start via the CLI arguments.

Start **Cloud Storage** emulator on port `6351`:

```sh
docker run --rm -p 6351:6351 nolanrs/gcpemulators:latest storage 
```

Start **Cloud Tasks** emulator on port `6352`:

```sh
docker run --rm -p 6352:6352 nolanrs/gcpemulators:latest cloudtasks
```

Once running, the emulators are available on `localhost` at the respective port on your host machine.

#### Connecting with gRPC

For low-level access you can connect directly with `google.golang.org/grpc`:

```go
conn, err := grpc.NewClient("localhost:6351", grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
	log.Fatalf("failed to create grpc client: %v", err)
}
defer conn.Close()

// Use conn with generated gRPC clients, e.g. storagepb.NewStorageClient(conn)
```

#### Connecting with Google Cloud clients (example: Cloud Storage)

When using the official Google Cloud clients, point them at the Docker emulator by
reusing the gRPC connection:

```go
ctx := context.Background()

// Connect to the storage emulator running in Docker on localhost:6351
conn, err := grpc.NewClient("localhost:6351", grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
	log.Fatalf("failed to create grpc client: %v", err)
}
defer conn.Close()

storageClient, err := storage.NewGRPCClient(ctx, option.WithGRPCConn(conn))
if err != nil {
	log.Fatalf("failed to create storage client: %v", err)
}
defer storageClient.Close()

// Now use the storage client as usual
err = storageClient.Bucket("my-bucket-name").Create(ctx, "12345", nil)
if err != nil {
	log.Fatalf("failed to create bucket: %v", err)
}
```

### Docker Compose

You can also run the emulator container via Docker Compose:

```yaml
services:
  gcpemulators:
    image: nolanrs/gcpemulators:latest
    command: >
      storage
    ports:
      - "6351:6351"
```

Then start it with:

```sh
docker compose up gcpemulators
```

Your application can then connect to `localhost:6351` exactly as in the examples above.