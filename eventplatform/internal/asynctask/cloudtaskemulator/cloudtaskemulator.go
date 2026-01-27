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
	"regexp"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nolanco/eventplatform/internal/asynctask/cloudtaskemulator/db"
	"github.com/nolanco/eventplatform/internal/legacy/slices"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server implements the Cloud Tasks gRPC service interface.
// It provides an emulator for GCP Cloud Tasks that can be used for local testing.
type Server struct {
	db     *mongo.Database
	logger *zap.Logger
	cloudtaskspb.UnimplementedCloudTasksServer
}

func NewServer(db *mongo.Database, logger *zap.Logger) *Server {
	return &Server{
		db:     db,
		logger: logger.Named("cloudtaskemulator"),
	}
}

const (
	CollectionQueues = "cloud_tasks_emulator_queues"
	CollectionTasks  = "cloud_tasks_emulator_tasks"
)

type CloudTaskEmulator struct {
	mongoDbURI          string
	dbName              string
	mongoDB             *mongo.Database
	logger              *zap.Logger
	port                string
	managedQueueIds     []primitive.ObjectID
	managedQueueIdsLock sync.RWMutex
}

func NewCloudTaskEmulator(mongoDBURI, dbBane string, logger *zap.Logger, grpcPort string) *CloudTaskEmulator {
	// Validate that port is a numeric string
	portValidator := regexp.MustCompile(`^[0-9]+$`)
	if !portValidator.MatchString(grpcPort) {
		panic(fmt.Sprintf("port must be a numeric string: %s", grpcPort))
	}

	// Validate that port is within valid range (1-65535)
	portNum, err := strconv.Atoi(grpcPort)
	if err != nil {
		panic(fmt.Sprintf("port must be a valid number: %s", grpcPort))
	}
	if portNum < 1 || portNum > 65535 {
		panic(fmt.Sprintf("port must be between 1 and 65535: %s", grpcPort))
	}

	return &CloudTaskEmulator{
		mongoDbURI:          mongoDBURI,
		dbName:              dbBane,
		logger:              logger.Named("cloudtaskemulator"),
		port:                ":" + grpcPort,
		managedQueueIds:     []primitive.ObjectID{},
		managedQueueIdsLock: sync.RWMutex{},
	}
}

func (s *CloudTaskEmulator) Run(ctx context.Context) error {
	defer s.logger.Sync()
	err := db.RunMigrations(s.mongoDbURI, s.dbName)
	if err != nil {
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
		return s.runGRPCServer(gctx)
	})

	// Start the background service
	g.Go(func() error {
		return s.runService(gctx)
	})

	// Wait for all goroutines to complete
	// Returns the first error from any goroutine, or nil if all complete successfully
	return g.Wait()
}

func (s *CloudTaskEmulator) runGRPCServer(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.port)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.port, err)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(insecure.NewCredentials()),
	)
	server := NewServer(s.mongoDB, s.logger)
	cloudtaskspb.RegisterCloudTasksServer(grpcServer, server)

	// Channel to signal server completion
	serverDone := make(chan error, 1)

	// Start server in goroutine
	go func() {
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

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// get all current queues
	cursor, err := s.mongoDB.Collection(CollectionQueues).Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to get queues: %w", err)
	}
	defer cursor.Close(ctx)

	var queues []db.Queue
	if err := cursor.All(ctx, &queues); err != nil {
		return fmt.Errorf("failed to get queues: %w", err)
	}

	wg := sync.WaitGroup{}
	for _, queue := range queues {
		wg.Add(1)
		go func(queue db.Queue) {
			defer wg.Done()
			err := s.processQueue(ctx, queue)
			if err != nil {
				s.logger.Error("failed to process queue", zap.Error(err))
			}
		}(queue)
	}

	newQueues, err := s.newQueues(ctx)
	if err != nil {
		return fmt.Errorf("failed to get new queues: %w", err)
	}

	for queue := range newQueues {
		wg.Add(1)
		go func(queue db.Queue) {
			defer wg.Done()
			err := s.processQueue(ctx, queue)
			if err != nil {
				s.logger.Error("failed to process queue", zap.Error(err))
			}
		}(queue)
	}

	wg.Wait()

	return nil
}

func (s *CloudTaskEmulator) newQueues(ctx context.Context) (<-chan db.Queue, error) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	queueChan := make(chan db.Queue, 100)
	go func() {
		defer close(queueChan)

		// Query immediately on start
		if err := s.queryAndSendNewQueues(ctx, queueChan); err != nil {
			s.logger.Error("failed to query new queues on startup", zap.Error(err))
		}

		for {
			select {
			case <-ctx.Done():
				// Context cancelled - shutdown requested
				return
			case <-ticker.C:
				// Query for new queues every 10 seconds
				if err := s.queryAndSendNewQueues(ctx, queueChan); err != nil {
					s.logger.Error("failed to query new queues", zap.Error(err))
					// Continue even on error - will retry on next tick
				}
			}
		}
	}()

	return queueChan, nil
}

func (s *CloudTaskEmulator) queryAndSendNewQueues(ctx context.Context, queueChan chan<- db.Queue) error {
	// Get the list of currently managed queue IDs (read lock)
	s.managedQueueIdsLock.RLock()
	managedIDs := make([]primitive.ObjectID, len(s.managedQueueIds))
	copy(managedIDs, s.managedQueueIds)
	s.managedQueueIdsLock.RUnlock()

	// Build query to find queues NOT in managedQueueIds
	filter := bson.M{}
	if len(managedIDs) > 0 {
		filter["_id"] = bson.M{"$nin": managedIDs}
	}

	// Query for new queues
	cursor, err := s.mongoDB.Collection(CollectionQueues).Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to query queues: %w", err)
	}
	defer cursor.Close(ctx)

	// Iterate through results and send to channel
	for cursor.Next(ctx) {
		var queue db.Queue
		if err := cursor.Decode(&queue); err != nil {
			s.logger.Error("failed to decode queue", zap.Error(err))
			continue
		}

		// Send queue to channel (non-blocking if channel is full)
		select {
		case queueChan <- queue:
			// Successfully sent
		case <-ctx.Done():
			// Context cancelled while trying to send
			return ctx.Err()
		default:
			// Channel is full - log warning but continue
			s.logger.Warn("queue channel is full, skipping queue",
				zap.String("queue_id", queue.Id.Hex()))
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	return nil
}

func (s *CloudTaskEmulator) processQueue(ctx context.Context, queue db.Queue) error {
	s.managedQueueIdsLock.Lock()
	s.managedQueueIds = append(s.managedQueueIds, queue.Id)
	s.managedQueueIdsLock.Unlock()

	defer func() {
		s.managedQueueIdsLock.Lock()
		s.managedQueueIds = slices.Filter(s.managedQueueIds, func(id primitive.ObjectID) bool {
			return id != queue.Id
		})
		s.managedQueueIdsLock.Unlock()
	}()

	queueLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	// keep the queue settings up to date.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				// get the queue from the database
				newQueue := db.Queue{}
				err := s.mongoDB.Collection(CollectionQueues).FindOne(ctx, bson.M{"_id": queue.Id}).Decode(&newQueue)
				if err != nil {
					s.logger.Error("failed to sync queue", zap.Error(err), zap.String("queue_name", queue.Name), zap.String("queue_id", queue.Id.Hex()))
					queueLock.Lock()
					queue.State = cloudtaskspb.Queue_STATE_UNSPECIFIED
					queueLock.Unlock()
					continue
				}
				queueLock.Lock()
				queue = newQueue
				queueLock.Unlock()
			}
		}
	}()

	// process all tasks on the queue. Get all the tasks, send to target with deadline, retry logic, etc.
	// support a maximum number of concurrent tasks to be processed and rate limit parameters.
	// query tasks
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		default:
			err := s.syncTasks(ctx, queue, 500)
			if err != nil {
				time.Sleep(5 * time.Second)
				if errors.Is(err, errNoTasksFound) {
					// no tasks found, wait a few seconds and try again
					continue
				}
				// log and wait a few seconds before trying again
				s.logger.Error("failed to sync tasks", zap.Error(err), zap.String("queue_name", queue.Name), zap.String("queue_id", queue.Id.Hex()))
				continue
			}
		}
	}
}

var errNoTasksFound = errors.New("no tasks found for queue")

func (s *CloudTaskEmulator) syncTasks(ctx context.Context, eQ db.Queue, maxTasks int64) error {

	maxConcurrentDispatches := 10
	if eQ.MaxConcurrentDispatches > 0 {
		maxConcurrentDispatches = int(eQ.MaxConcurrentDispatches)
	}

	taskChan := make(chan *cloudtaskspb.Task, maxConcurrentDispatches)

	// Query tasks for this queue using cursor
	cursor, err := s.mongoDB.Collection(CollectionTasks).Find(ctx, bson.M{
		"queue_id": eQ.Id,
	}, &options.FindOptions{
		Sort:  bson.D{{Key: "created_at", Value: 1}},
		Limit: &maxTasks,
	})

	if err != nil {
		return fmt.Errorf("failed to query tasks for queue %s: %w", eQ.Name, err)
	}
	defer cursor.Close(ctx)
	if cursor.RemainingBatchLength() == 0 {
		return errNoTasksFound
	}

	// Send tasks to channel one at a time
	go func() {
		defer close(taskChan)
		for cursor.Next(ctx) {
			var task cloudtaskspb.Task
			if err := cursor.Decode(&task); err != nil {
				s.logger.Error("failed to decode task",
					zap.Error(err),
					zap.String("queue_name", eQ.Name))
				continue
			}

			// Send task to channel, respecting context cancellation
			select {
			case taskChan <- &task:
				// Successfully sent
			case <-ctx.Done():
				// Context cancelled while trying to send
				return
			}
		}

		// Check for cursor errors
		if err := cursor.Err(); err != nil {
			s.logger.Error("cursor error while querying tasks",
				zap.Error(err),
				zap.String("queue_name", eQ.Name))
		}
	}()

	wg := sync.WaitGroup{}
	for t := range taskChan {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("panic while processing task", zap.Any("task", t))
				}
			}()
			err := s.processTask(ctx, eQ, t)
			if err != nil {
				s.logger.Error("failed to process task", zap.Error(err))
			}
		}()
	}
	wg.Wait()
	return nil
}

func (s *CloudTaskEmulator) processTask(ctx context.Context, eQ db.Queue, t *cloudtaskspb.Task) error {

	return nil
}
