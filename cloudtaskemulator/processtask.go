package cloudtaskemulator

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
)

// httpStatusCodeToGRPCCode maps HTTP status codes to gRPC status codes.
// the following http status codes can be retried if the retry configuration allows,
// all others will be treated as permanent failures:
//
// - http.StatusInternalServerError
// - http.StatusTooManyRequests
// - http.StatusBadGateway
// - http.StatusServiceUnavailable
// - http.StatusGatewayTimeout
func httpStatusCodeToGRPCCode(httpCode int) codes.Code {
	switch httpCode {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	case http.StatusNotImplemented:
		return codes.Unimplemented
	default:
		return codes.Unknown
	}
}

// buildHTTPRequest constructs an http.Request from the task's HTTP request configuration.
// TODO: Currently there is no support for OAuth tokens or OIDC tokens.
func buildHTTPRequest(ctx context.Context, httpReq *db.Task_HttpRequest) (*http.Request, error) {
	if httpReq == nil {
		return nil, fmt.Errorf("task has no HTTP request payload")
	}

	method := httpReq.HttpMethod.String()
	if method == "" || method == cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED.String() {
		method = http.MethodPost
	}

	bodyReader := bytes.NewReader(httpReq.Body)
	req, err := http.NewRequestWithContext(ctx, method, httpReq.Url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to build HTTP request for task: %w", err)
	}

	for k, v := range httpReq.Headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

// deliverTaskToTarget executes the HTTP request for the task and returns the response status.
// the status is always internal for errors that happen on the emulators side of the request.
// besides on exceptions, ctx cancellations from us, or timeouts because the target took too long
// to respond are always returned as codes.DeadlineExceeded.
func deliverTaskToTarget(ctx context.Context, task *db.Task) (*status.Status, error) {
	req, err := buildHTTPRequest(ctx, task.HttpRequest)
	if err != nil {
		return &status.Status{
			Code:    int32(codes.Internal),
			Message: err.Error(),
		}, err
	}

	// Derive a context with timeout from DispatchDeadline (or use a default of 10 minutes).
	dispatchDeadline := task.DispatchDeadline
	if dispatchDeadline == nil {
		defaultDeadline := 10 * time.Minute
		dispatchDeadline = &defaultDeadline
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, *dispatchDeadline)
	defer cancel()

	// Execute the request with an HTTP client that respects the context.
	resp, err := http.DefaultClient.Do(req.WithContext(ctxWithTimeout))
	if err != nil {
		errMsg := fmt.Errorf("failed to execute HTTP request for task: %w", err)
		return &status.Status{
			Code:    int32(codes.Internal),
			Message: errMsg.Error(),
		}, errMsg
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return &status.Status{
			Code:    int32(codes.OK),
			Message: "HTTP request executed successfully",
		}, nil
	}

	// Map HTTP status code to gRPC status code.
	errMsg := fmt.Errorf("failed to execute HTTP request, worker returned with status code %d", resp.StatusCode)
	grpcCode := httpStatusCodeToGRPCCode(resp.StatusCode)
	return &status.Status{
		Code:    int32(grpcCode),
		Message: errMsg.Error(),
	}, errMsg
}

// createTaskAttempt creates a Task_Attempt record for the current dispatch.
func createTaskAttempt(task *db.Task, dispatchTime, responseTime time.Time, respStatus *status.Status) db.Task_Attempt {
	return db.Task_Attempt{
		ScheduleTime:          task.ScheduleTime,
		DispatchTime:          &dispatchTime,
		ResponseTime:          &responseTime,
		ResponseStatusCode:    respStatus.Code,
		ResponseStatusMessage: respStatus.Message,
	}
}

// isRetryableStatusCode returns true if the gRPC status code indicates a retryable error.
// the following http status codes and grpc status codes can be retried if the retry configuration allows,
// all others will be treated as permanent failures:
//
//	http:
//
// - http.StatusInternalServerError
// - http.StatusTooManyRequests
// - http.StatusBadGateway
// - http.StatusServiceUnavailable
// - http.StatusGatewayTimeout
//
//	which map to the following grpc status codes (see httpStatusCodeToGRPCCode):
//
// - codes.Internal
// - codes.Unavailable
// - codes.DeadlineExceeded
// - codes.ResourceExhausted
func isRetryableStatusCode(code int32) bool {
	switch code {
	case int32(codes.Internal),
		int32(codes.Unavailable),
		int32(codes.DeadlineExceeded),
		int32(codes.ResourceExhausted):
		return true
	default:
		return false
	}
}

// getFirstScheduleTime returns the schedule time of the first attempt, or the task's schedule time if no attempts exist.
func getFirstScheduleTime(task *db.Task) *time.Time {
	if task.FirstAttempt != nil && task.FirstAttempt.ScheduleTime != nil {
		return task.FirstAttempt.ScheduleTime
	}
	return task.ScheduleTime
}

// shouldRetryTask determines if a failed task should be retried based on queue configuration and attempt history.
func shouldRetryTask(queue *db.Queue, task *db.Task, respStatus *status.Status) bool {
	// Check if the error is retryable.
	if !isRetryableStatusCode(respStatus.Code) {
		return false
	}

	// Check if max retry duration has been reached.
	if queue.MaxRetryDuration != nil && *queue.MaxRetryDuration != 0 {
		firstScheduleTime := getFirstScheduleTime(task)
		if firstScheduleTime != nil {
			retryDeadline := firstScheduleTime.Add(*queue.MaxRetryDuration)
			if time.Now().After(retryDeadline) {
				return false
			}
		}
	}

	// Check if we have remaining attempts.
	hasUnlimitedAttempts := queue.MaxAttempts < 0
	hasRemainingAttempts := queue.MaxAttempts > task.DispatchCount+1
	return hasUnlimitedAttempts || hasRemainingAttempts
}

// processTask processes a task by dispatching it to its target and updating its status.
// It can be called from both CloudTaskEmulator and Server.
func processTask(ctx context.Context, mongoDB *mongo.Database, logger *zap.Logger, queue *db.Queue, task *db.Task) error {
	logger.Debug("processing task",
		zap.String("queue_name", queue.Name),
		zap.String("queue_id", queue.Id.Hex()),
		zap.String("task_id", task.Name),
	)

	// Execute the HTTP request and capture timing.
	dispatchTime := time.Now()
	deliveryStatus, deliveryError := deliverTaskToTarget(ctx, task)
	responseTime := time.Now()

	// Create attempt record (used for both first_attempt and last_attempt).
	attempt := createTaskAttempt(task, dispatchTime, responseTime, deliveryStatus)

	// Build updates map with common fields.
	updates := bson.M{
		"dispatch_count":  task.DispatchCount + 1,
		"last_attempt":    attempt,
		"lock_expires_at": nil,
		"updated_at":      time.Now(),
	}

	// Record first_attempt if this is the first dispatch.
	if task.FirstAttempt == nil {
		updates["first_attempt"] = attempt
	}

	// Handle success case.
	if deliveryError == nil {
		updates["status"] = db.TaskStatusSucceeded
		updates["response_count"] = task.ResponseCount + 1
		updates["deleted_at"] = time.Now()
	} else {
		// Handle failure case.
		// Only increment response_count if we got a response from the worker (not an internal error or deadline exceeded).
		if deliveryStatus.Code != int32(codes.Internal) {
			updates["response_count"] = task.ResponseCount + 1
		}

		// Determine if task should be retried or permanently failed.
		if shouldRetryTask(queue, task, deliveryStatus) {
			nextRetryTime := calculateNextRetryTime(queue, int(task.DispatchCount+1))
			updates["schedule_time"] = nextRetryTime
			updates["status"] = db.TaskStatusPending
		} else {
			updates["status"] = db.TaskStatusFailed
			updates["deleted_at"] = time.Now()
		}
	}

	// Persist all task changes back to MongoDB.
	mResult, deliveryError := mongoDB.Collection(db.CollectionTasks).UpdateOne(
		ctx,
		bson.M{"_id": task.Id},
		bson.M{"$set": updates},
	)
	if deliveryError != nil {
		return fmt.Errorf("failed to update task: %w", deliveryError)
	}
	if mResult.MatchedCount == 0 {
		return fmt.Errorf("task not found when updating: %w", mongo.ErrNoDocuments)
	}

	logger.Debug("task finished",
		zap.String("queue_name", queue.Name),
		zap.String("queue_id", queue.Id.Hex()),
		zap.String("task_id", task.Name),
	)
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
