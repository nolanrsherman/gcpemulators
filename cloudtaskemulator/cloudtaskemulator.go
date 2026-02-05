// This package is an emualator for GCP Cloud Tasks.
// It can be used to test cloud tasks locally and supports
// the most common features of Cloud Tasks.
// 1. Cloud task API - Managing Queues and Tasks.
// 2. Delivery to task targets with HTTP, with all Retry logic, timeouts and Rate Limiting.
// 3. Implementation of features like Task Deduplication, and Rate Limiting.
package cloudtaskemulator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/golang-migrate/migrate/v4"
	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"
	"github.com/nolanrsherman/gcpemulators/gcpemulators/gcpemulatorspb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CloudTaskEmulator struct {
	mongoDbURI      string
	dbName          string
	mongoDB         *mongo.Database
	logger          *zap.Logger
	port            int
	managedQueueIds map[primitive.ObjectID]struct{}
	cancelFn        context.CancelFunc
	cloudtaskspb.UnimplementedCloudTasksServer
	gcpemulatorspb.UnimplementedGcpEmulatorServer
}

func (s *CloudTaskEmulator) Port() int {
	return s.port
}

func NewCloudTaskEmulator(mongoDBURI, dbBane string, logger *zap.Logger, grpcPort int) *CloudTaskEmulator {
	// Validate that port is a numeric string

	return &CloudTaskEmulator{
		mongoDbURI:      mongoDBURI,
		dbName:          dbBane,
		logger:          logger.Named("cloudtaskemulator"),
		port:            grpcPort,
		managedQueueIds: make(map[primitive.ObjectID]struct{}),
		cancelFn:        func() {},
	}
}

func (s *CloudTaskEmulator) Run(ctx context.Context) error {
	// TODO: create database with name if it doesn't already exist.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.cancelFn = cancel
	defer s.logger.Sync()
	err := db.RunMigrations(s.mongoDbURI, s.dbName)
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	mConn, err := db.NewConnection(context.Background(), s.mongoDbURI)
	if err != nil {
		return fmt.Errorf("failed to connect to mongo: %w", err)
	}
	s.mongoDB = mConn.Database(s.dbName)
	defer mConn.Disconnect(ctx)

	// Use errgroup to manage multiple goroutines
	// If any goroutine returns an error, all others will be cancelled via context
	g, gctx := errgroup.WithContext(ctx)

	// Start the gRPC server
	g.Go(func() error {
		defer s.logger.Debug("goroutine stopped: runGRPCServer")
		return s.runGRPCServer(gctx)
	})

	// Start the background service
	g.Go(func() error {
		defer s.logger.Debug("goroutine stopped: runService")
		return s.runService(gctx)
	})

	// Wait for all goroutines to complete
	// Returns the first error from any goroutine, or nil if all complete successfully
	return g.Wait()
}

func (s *CloudTaskEmulator) runGRPCServer(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on %d: %w", s.port, err)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)

	cloudtaskspb.RegisterCloudTasksServer(grpcServer, s)
	gcpemulatorspb.RegisterGcpEmulatorServer(grpcServer, s)

	// Channel to signal server completion
	serverDone := make(chan error, 1)

	// Start server in goroutine
	go func() {
		defer s.logger.Debug("goroutine stopped: grpcServer.Serve", zap.Int("port", s.port))
		err := grpcServer.Serve(lis)
		// Always signal completion, even on normal shutdown
		// grpc.ErrServerStopped is returned on normal shutdown, which is not an error
		if err != nil && err != grpc.ErrServerStopped {
			serverDone <- err
		} else {
			serverDone <- nil
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		// Graceful shutdown
		stopped := make(chan struct{})
		go func() {
			defer s.logger.Debug("goroutine stopped: grpcServer.GracefulStop")
			grpcServer.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			// Graceful stop completed
		case <-time.After(30 * time.Second):
			// Force stop after timeout
			s.logger.Warn("graceful stop timeout, forcing stop")
			grpcServer.Stop()
		}

		// Close listener after server stops
		if err := lis.Close(); err != nil {
			s.logger.Error("failed to close listener", zap.Error(err))
		}

		// Wait for server goroutine to finish
		<-serverDone
		return ctx.Err()

	case err := <-serverDone:
		// Server exited (error or normal)
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
		return nil
	}
}

func (s *CloudTaskEmulator) runService(ctx context.Context) error {
	// This goroutine is managed by errgroup, so it should return an error
	// when it exits (or nil if it completes successfully)
	// The context will be cancelled if any other goroutine in the group fails

	// Create an errgroup to manage all queue processing goroutines
	// This allows errors from any goroutine to be propagated up
	g, gctx := errgroup.WithContext(ctx)

	// get all current queues
	queues, err := db.SelectAllQueuesWithDeletedAtNotSet(gctx, s.mongoDB)
	if err != nil {
		return fmt.Errorf("failed to get queues: %w", err)
	}

	// Each Queue is going to get its own go routine to process the queue.
	for _, queue := range queues {
		// Track this queue as managed
		s.managedQueueIds[queue.Id] = struct{}{}
		g.Go(func() error {
			defer s.logger.Debug("goroutine stopped: processQueue (initial)", zap.String("queue_id", queue.Id.Hex()), zap.String("queue_name", queue.Name))
			err := s.processQueue(gctx, queue)
			if err != nil {
				s.logger.Error("failed to process queue", zap.Error(err), zap.String("queue_id", queue.Id.Hex()))
				return fmt.Errorf("failed to process queue %s: %w", queue.Id.Hex(), err)
			}
			return nil
		})
	}

	// In a sub process we will keep querying for new queues every 10s and start a new routine for them too.
	g.Go(func() error {
		defer s.logger.Debug("goroutine stopped: newQueueDiscovery")
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			case <-ticker.C:
				// Query for new queues every 10 seconds
				managedQueueIds := make([]primitive.ObjectID, 0, len(s.managedQueueIds))
				for id := range s.managedQueueIds {
					managedQueueIds = append(managedQueueIds, id)
				}
				newQueues, err := db.SelectQueuesWithIDNotInList(gctx, s.mongoDB, managedQueueIds)
				if err != nil {
					s.logger.Error("failed to query new queues", zap.Error(err))
					return fmt.Errorf("failed to query new queues: %w", err)
				}
				for _, queue := range newQueues {
					select {
					case <-gctx.Done():
						return gctx.Err()
					default:
					}
					// Check if queue is already being managed (with lock protection)
					if _, ok := s.managedQueueIds[queue.Id]; ok {
						s.logger.Warn("queue already managed, skipping", zap.String("queue_id", queue.Id.Hex()))
						continue
					}
					// update that this is a queue id we are aware of.
					s.managedQueueIds[queue.Id] = struct{}{}
					// start a new go routine to process the queue using the same errgroup
					g.Go(func() error {
						defer s.logger.Debug("goroutine stopped: processQueue (discovered)", zap.String("queue_id", queue.Id.Hex()), zap.String("queue_name", queue.Name))
						err := s.processQueue(gctx, queue)
						if err != nil {
							s.logger.Error("failed to process queue", zap.Error(err), zap.String("queue_id", queue.Id.Hex()))
							return fmt.Errorf("failed to process queue %s: %w", queue.Id.Hex(), err)
						}
						return nil
					})
				}
			}
		}
	})

	// Wait for all goroutines to complete
	// Returns the first error from any goroutine, or nil if all complete successfully
	return g.Wait()
}

func (s *CloudTaskEmulator) processQueue(ctx context.Context, queue *db.Queue) error {
	s.logger.Debug("processing queue", zap.String("queue_id", queue.Id.Hex()), zap.String("queue_name", queue.Name))
	// Create a cancellable context for this queue. This allows us to cleanly
	// shut down all goroutines when the queue is deleted or the parent context
	// is cancelled.
	queueCtx, cancelQueueCtx := context.WithCancel(ctx)
	defer cancelQueueCtx() // Ensure context is cancelled when function returns

	queueLock := sync.Mutex{}
	wg := sync.WaitGroup{}

	// keep the queue settings up to date.
	syncQueueErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			s.logger.Debug("goroutine stopped: queueSync", zap.String("queue_id", queue.Id.Hex()), zap.String("queue_name", queue.Name))
		}()
		for {
			select {
			case <-queueCtx.Done():
				return
			case <-time.After(10 * time.Second):
				// get the queue from the database
				newQueue := db.Queue{}
				err := s.mongoDB.Collection(db.CollectionQueues).FindOne(queueCtx, bson.M{"_id": queue.Id}).Decode(&newQueue)
				if err != nil {
					s.logger.Error("failed to sync queue", zap.Error(err), zap.String("queue_name", queue.Name), zap.String("queue_id", queue.Id.Hex()))
					queueLock.Lock()
					queue.State = cloudtaskspb.Queue_STATE_UNSPECIFIED
					queueLock.Unlock()
					syncQueueErr <- err
				}
				queueLock.Lock()
				queue = &newQueue
				queueLock.Unlock()
			}
		}
	}()

	// process all tasks on the queue. Get all the tasks, send to target with deadline, retry logic, etc.
	// support a maximum number of concurrent tasks to be processed and rate limit parameters.
	// query tasks with exponential backoff up to a maximum of 5 seconds
	backoffDuration := 100 * time.Millisecond
	maxBackoff := 5 * time.Second
	for {
		select {
		case <-queueCtx.Done():
			// Context cancelled (either parent cancelled or queue deleted)
			wg.Wait()
			return queueCtx.Err()
		case err := <-syncQueueErr:
			cancelQueueCtx()
			wg.Wait()
			return fmt.Errorf("failed to sync queue: %w", err)
		default:
			// Check if queue was deleted (need to check under lock)
			queueLock.Lock()
			isDeleted := queue.DeletedAt != nil
			currentState := queue.State
			queueLock.Unlock()

			if isDeleted {
				// Queue is deleted - cancel the queue context to signal all
				// goroutines to stop, then wait for them to finish.
				cancelQueueCtx()
				wg.Wait()
				return nil
			}
			if currentState != cloudtaskspb.Queue_RUNNING {
				// queue is not running, wait a few seconds and try again
				time.Sleep(1 * time.Second)
				continue
			}

			task, err := db.SelectNextTaskAndLock(ctx, s.mongoDB, queue.Id)
			if err != nil {
				return fmt.Errorf("failed to select next task: %w", err)
			}
			if task == nil {
				// no tasks found, wait a few seconds and try again
				time.Sleep(backoffDuration)
				// Exponential backoff with cap at maxBackoff
				backoffDuration *= 2
				if backoffDuration > maxBackoff {
					backoffDuration = maxBackoff
				}
				continue
			}
			// Reset backoff after successfully finding a task
			backoffDuration = 100 * time.Millisecond
			// process the task
			err = processTask(queueCtx, s.mongoDB, s.logger, queue, task)
			if err != nil {
				return fmt.Errorf("failed to process task: %w", err)
			}
		}

	}
}

func (s *CloudTaskEmulator) Readiness(context.Context, *gcpemulatorspb.ReadinessRequest) (*gcpemulatorspb.ReadinessResponse, error) {
	return &gcpemulatorspb.ReadinessResponse{
		Ready:   true,
		Message: "Emulator is ready and accepting requests",
	}, nil
}
