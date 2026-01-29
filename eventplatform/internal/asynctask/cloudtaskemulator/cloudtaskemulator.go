// This package is an emualator for GCP Cloud Tasks.
// It can be used to test cloud tasks locally and supports
// the most common features of Cloud Tasks.
// 1. Cloud task API - Managing Queues and Tasks.
// 2. Delivery to task targets with HTTP, with all Retry logic, timeouts and Rate Limiting.
// 3. Implementation of features like Task Deduplication, and Rate Limiting.
package cloudtaskemulator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/golang-migrate/migrate/v4"
	"github.com/nolanco/eventplatform/internal/asynctask/cloudtaskemulator/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

type CloudTaskEmulator struct {
	mongoDbURI           string
	dbName               string
	mongoDB              *mongo.Database
	logger               *zap.Logger
	port                 string
	managedQueueIds      map[primitive.ObjectID]struct{}
	specialEventChannels specialEventChannels
}

type specialEventChannels struct {
	cloudTaskEmulatorReady chan struct{}
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
		mongoDbURI:      mongoDBURI,
		dbName:          dbBane,
		logger:          logger.Named("cloudtaskemulator"),
		port:            ":" + grpcPort,
		managedQueueIds: make(map[primitive.ObjectID]struct{}),
		specialEventChannels: specialEventChannels{
			cloudTaskEmulatorReady: make(chan struct{}, 1),
		},
	}
}

func (s *CloudTaskEmulator) Run(ctx context.Context) error {
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
	s.specialEventChannels.cloudTaskEmulatorReady <- struct{}{}
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
		defer s.logger.Debug("goroutine stopped: grpcServer.Serve", zap.String("port", s.port))
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
		ticker := time.NewTicker(10 * time.Second)
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

			task, err := s.selectNextTask(queueCtx, queue)
			if err != nil {
				if errors.Is(err, errNoTasksFound) {
					// no tasks found, wait a few seconds and try again
					time.Sleep(backoffDuration)
					// Exponential backoff with cap at maxBackoff
					backoffDuration *= 2
					if backoffDuration > maxBackoff {
						backoffDuration = maxBackoff
					}
					continue
				}
				return fmt.Errorf("failed to select next task: %w", err)
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

var errNoTasksFound = errors.New("no tasks found for queue")

func (s *CloudTaskEmulator) selectNextTask(ctx context.Context, eQ *db.Queue) (*db.Task, error) {
	task, err := db.SelectNextTaskAndLock(ctx, s.mongoDB, eQ.Id)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errNoTasksFound
		}
		return nil, fmt.Errorf("failed to select next task: %w", err)
	}
	return task, nil
}

// processTask processes a task by dispatching it to its target and updating its status.
// It can be called from both CloudTaskEmulator and Server.
func processTask(ctx context.Context, mongoDB *mongo.Database, logger *zap.Logger, eQ *db.Queue, t *db.Task) error {
	logger.Debug("processing task", zap.String("queue_name", eQ.Name), zap.String("queue_id", eQ.Id.Hex()), zap.String("task_id", t.Name))

	deliverTaskToTarget := func(ctx context.Context, t *db.Task) (status.Status, error) {

		// Implement HTTP dispatch based on t.HttpRequest and DispatchDeadline:
		//  1. Build an http.Request from t.HttpRequest:
		//     - URL: t.HttpRequest.Url
		//     - Method: t.HttpRequest.HttpMethod
		//     - Headers: t.HttpRequest.Headers
		//     - Body: t.HttpRequest.Body
		if t.HttpRequest == nil {
			errMsg := fmt.Errorf("task has no HTTP request payload")
			return status.Status{
				Code:    int32(codes.Internal),
				Message: errMsg.Error(),
			}, errMsg
		}
		bodyReader := bytes.NewReader(t.HttpRequest.Body)
		method := t.HttpRequest.HttpMethod.String()
		if method == "" || method == cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED.String() {
			method = http.MethodPost
		}
		req, err := http.NewRequestWithContext(ctx, method, t.HttpRequest.Url, bodyReader)
		if err != nil {
			errMsg := fmt.Errorf("failed to build HTTP request for task: %w", err)
			return status.Status{
				Code:    int32(codes.Internal),
				Message: errMsg.Error(),
			}, errMsg
		}
		for k, v := range t.HttpRequest.Headers {
			req.Header.Set(k, v)
		}

		//  2. Derive a context with timeout from t.DispatchDeadline (or use a default
		//     of ~10 minutes for HTTP tasks) so the request is cancelled if it runs
		//     longer than the configured deadline.
		dispatchDeadline := t.DispatchDeadline
		if dispatchDeadline == nil {
			d := 10 * time.Minute
			dispatchDeadline = &d
		}
		ctxWithTimeout, cancel := context.WithTimeout(ctx, *dispatchDeadline)
		defer cancel()
		//  3. Execute the request with an HTTP client that respects the context.
		resp, err := http.DefaultClient.Do(req.WithContext(ctxWithTimeout))
		if err != nil {
			errMsg := fmt.Errorf("failed to execute HTTP request for task: %w", err)
			if errors.Is(err, context.DeadlineExceeded) {
				return status.Status{
					Code:    int32(codes.DeadlineExceeded),
					Message: errMsg.Error(),
				}, errMsg
			}
			return status.Status{
				Code:    int32(codes.Internal),
				Message: errMsg.Error(),
			}, errMsg
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			errMsg := fmt.Errorf("failed to execute HTTP request, worker returned with status code %d", resp.StatusCode)
			statusCode := int32(codes.Unknown)
			switch resp.StatusCode {
			case http.StatusBadRequest:
				statusCode = int32(codes.InvalidArgument)
			case http.StatusUnauthorized:
				statusCode = int32(codes.Unauthenticated)
			case http.StatusForbidden:
				statusCode = int32(codes.PermissionDenied)
			case http.StatusNotFound:
				statusCode = int32(codes.NotFound)
			case http.StatusTooManyRequests:
				statusCode = int32(codes.ResourceExhausted)
			case http.StatusInternalServerError:
				statusCode = int32(codes.Unavailable)
			case http.StatusBadGateway:
				statusCode = int32(codes.Unavailable)
			case http.StatusServiceUnavailable:
				statusCode = int32(codes.Unavailable)
			case http.StatusGatewayTimeout:
				statusCode = int32(codes.DeadlineExceeded)
			case http.StatusNotImplemented:
				statusCode = int32(codes.Unimplemented)
			}
			return status.Status{
				Code:    statusCode,
				Message: errMsg.Error(),
			}, errMsg
		}

		return status.Status{
			Code:    int32(codes.OK),
			Message: "HTTP request executed successfully",
		}, nil
	}

	dispatchTime := time.Now()
	respStatus, err := deliverTaskToTarget(ctx, t)
	responseTime := time.Now()
	updates := bson.M{}
	if err == nil {
		//  4. On success:
		//     - Update the task's Status to TaskStatusSucceeded.
		updates["status"] = db.TaskStatusSucceeded
		//     - Increment DispatchCount and ResponseCount.
		updates["dispatch_count"] = t.DispatchCount + 1
		updates["response_count"] = t.ResponseCount + 1
		//     - Record FirstAttempt / LastAttempt timestamps if this is the first
		//       time we are dispatching or to track the latest attempt.
		if t.FirstAttempt == nil {
			updates["first_attempt"] = db.Task_Attempt{
				ScheduleTime:          t.ScheduleTime,
				DispatchTime:          &dispatchTime,
				ResponseTime:          &responseTime,
				ResponseStatusCode:    respStatus.Code,
				ResponseStatusMessage: respStatus.Message,
			}
		}
		updates["last_attempt"] = db.Task_Attempt{
			ScheduleTime:          t.ScheduleTime,
			DispatchTime:          &dispatchTime,
			ResponseTime:          &responseTime,
			ResponseStatusCode:    respStatus.Code,
			ResponseStatusMessage: respStatus.Message,
		}
		//     - Clear LockExpiresAt to indicate the task is no longer leased.
		updates["lock_expires_at"] = nil
		updates["deleted_at"] = time.Now()
	} else {
		//  5. On failure (including context deadline exceeded or transport errors):
		//     - Increment DispatchCount.
		updates["dispatch_count"] = t.DispatchCount + 1
		// Update response count
		if respStatus.Code != int32(codes.Internal) {
			// only count the response count if the response is not an internal error
			// which means we got a response from the worker.
			updates["response_count"] = t.ResponseCount + 1
		}
		//     - Record FirstAttempt / LastAttempt if this is the first
		//       time we are dispatching or to track the latest attempt.
		if t.FirstAttempt == nil {
			updates["first_attempt"] = db.Task_Attempt{
				ScheduleTime:          t.ScheduleTime,
				DispatchTime:          &dispatchTime,
				ResponseTime:          &responseTime,
				ResponseStatusCode:    respStatus.Code,
				ResponseStatusMessage: respStatus.Message,
			}
		}
		updates["last_attempt"] = db.Task_Attempt{
			ScheduleTime:          t.ScheduleTime,
			DispatchTime:          &dispatchTime,
			ResponseTime:          &responseTime,
			ResponseStatusCode:    respStatus.Code,
			ResponseStatusMessage: respStatus.Message,
		}

		isStatusRetryable := false
		switch respStatus.Code {
		case int32(codes.Internal),
			int32(codes.Unavailable),
			int32(codes.DeadlineExceeded),
			int32(codes.ResourceExhausted):
			isStatusRetryable = true
		}

		retryDurationReached := false
		if eQ.MaxRetryDuration != nil && *eQ.MaxRetryDuration != 0 {
			var firstScheduleTime time.Time
			if t.FirstAttempt != nil && t.FirstAttempt.ScheduleTime != nil {
				firstScheduleTime = *t.FirstAttempt.ScheduleTime
			} else if t.ScheduleTime != nil {
				firstScheduleTime = *t.ScheduleTime
			}
			retryDurationReached = firstScheduleTime.Add(*eQ.MaxRetryDuration).Before(time.Now())
		}
		//     - If t.DispatchCount < eQ.MaxAttempts, compute a new RetryAfter based
		//       on the queue's RetryConfig and set Status back to TaskStatusPending
		//       with a new ScheduleTime, so it will be retried later
		//     - Cloud Tasks will attempt the task `max_attempts` times (that is, if the
		//       first attempt fails, then there will be `max_attempts - 1` retries). Must be >= -1.
		//       If unspecified when the queue is created, Cloud Tasks will pick the default.
		//       -1 indicates unlimited attempts.
		hasUnlimitedAttempts := eQ.MaxAttempts < 0
		hasRemainingAttempts := eQ.MaxAttempts > t.DispatchCount+1
		if (hasUnlimitedAttempts || hasRemainingAttempts) && !retryDurationReached && isStatusRetryable {
			// Compute next retry time based on the queue's retry configuration
			nextRetryTime := calculateNextRetryTime(eQ, int(t.DispatchCount+1))
			updates["schedule_time"] = nextRetryTime
			updates["status"] = db.TaskStatusPending
		} else {
			//     - If retries are exhausted,
			//     - Update the task's Status to TaskStatusFailed.
			updates["status"] = db.TaskStatusFailed
			updates["deleted_at"] = time.Now()
		}

	}
	// clear the lock in all cases
	updates["lock_expires_at"] = nil
	updates["updated_at"] = time.Now()

	//  6. Persist all task changes back to MongoDB (UpdateOne by _id), ensuring
	//     we do not regress LockExpiresAt or Status in the presence of races.
	mResult, err := mongoDB.Collection(db.CollectionTasks).UpdateOne(
		ctx,
		bson.M{"_id": t.Id},
		bson.M{"$set": updates},
	)
	if err != nil {
		return fmt.Errorf("failed to update task: %w", err)
	}
	if mResult.MatchedCount == 0 {
		return fmt.Errorf("task not found when updating: %w", mongo.ErrNoDocuments)
	}

	logger.Debug("task finished", zap.String("queue_name", eQ.Name), zap.String("queue_id", eQ.Id.Hex()), zap.String("task_id", t.Name))
	return nil
}

func calculateNextRetryTime(eQ *db.Queue, attemptCount int) time.Time {
	// attemptCount is 1-based (first retry after the initial attempt is 1).
	// We translate it into a zero-based retry index k for the backoff formula.
	if attemptCount < 1 {
		attemptCount = 1
	}
	k := attemptCount - 1

	// Determine effective min_backoff and max_backoff, falling back to the
	// emulator defaults used when creating queues if not explicitly set.
	minBackoff := time.Second
	if eQ.MinBackoff != nil {
		minBackoff = *eQ.MinBackoff
	}
	maxBackoff := time.Minute
	if eQ.MaxBackoff != nil {
		maxBackoff = *eQ.MaxBackoff
	}

	// Effective max_doublings (cannot be negative).
	maxDoublings := int(eQ.MaxDoublings)
	if maxDoublings < 0 {
		maxDoublings = 0
	}

	// Compute the backoff duration following the Cloud Tasks semantics:
	//   - Start at min_backoff.
	//   - Double the delay up to max_doublings times.
	//   - After that, increase linearly by the capped delay.
	//   - Never exceed max_backoff.
	var delay time.Duration
	if k <= maxDoublings {
		// Exponential phase: delay = min_backoff * 2^k
		delay = minBackoff * time.Duration(1<<k)
	} else {
		// Linear phase:
		//  base = min_backoff * 2^max_doublings
		//  delay = base + (k - max_doublings) * base
		base := minBackoff * time.Duration(1<<maxDoublings)
		extra := k - maxDoublings
		delay = base + time.Duration(extra)*base
	}

	// Clamp to max_backoff.
	if delay > maxBackoff {
		delay = maxBackoff
	}

	// NOTE: eQ.MaxRetryDuration (task age limit) is enforced at the task level
	// using the first-attempt timestamp; since this helper does not know that
	// timestamp, we intentionally do not enforce it here.

	return time.Now().Add(delay)
}
