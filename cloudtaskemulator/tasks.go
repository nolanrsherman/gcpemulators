package cloudtaskemulator

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Lists the tasks in a queue.
//
// By default, only the [BASIC][google.cloud.tasks.v2.Task.View.BASIC] view is
// retrieved due to performance considerations;
// [response_view][google.cloud.tasks.v2.ListTasksRequest.response_view]
// controls the subset of information which is returned.
//
// The tasks may be returned in any order. The ordering may change at any
// time.
func (s *CloudTaskEmulator) ListTasks(ctx context.Context, req *cloudtaskspb.ListTasksRequest) (*cloudtaskspb.ListTasksResponse, error) {
	// Validate parent (queue name)
	parentName := req.GetParent()
	if parentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "parent name is required")
	}
	if err := validateQueueName(parentName); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent name: %s", err.Error())
	}

	// Verify the queue exists and get its ID for filtering tasks
	queue, err := db.SelectQueueByName(ctx, s.mongoDB, parentName)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "queue %s not found", parentName)
		}
		return nil, status.Errorf(codes.Internal, "failed to get queue: %v", err)
	}

	// Determine page size (clamp to maximum of 1000 per Cloud Tasks API)
	pageSize := req.GetPageSize()
	if pageSize <= 0 {
		pageSize = 1000 // Default to maximum per Cloud Tasks API
	}
	if pageSize > 1000 {
		pageSize = 1000
	}

	// Decode page token as an integer offset
	var offset int64
	if token := strings.TrimSpace(req.GetPageToken()); token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token")
		}
		offset = int64(n)
	}

	col := s.mongoDB.Collection(db.CollectionTasks)

	// Filter tasks by queue id and exclude soft-deleted tasks
	filter := bson.M{
		"queue_id":   queue.Id,
		"deleted_at": bson.M{"$exists": false},
	}

	// Fetch one extra document to determine if there is a next page
	limit := int64(pageSize) + 1
	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
		SetSkip(offset).
		SetLimit(limit)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list tasks: %v", err)
	}
	defer cursor.Close(ctx)

	var dbTasks []db.Task
	if err := cursor.All(ctx, &dbTasks); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode tasks: %v", err)
	}

	hasMore := int32(len(dbTasks)) > pageSize
	if hasMore {
		dbTasks = dbTasks[:pageSize]
	}

	// Determine response view (default to BASIC if unspecified)
	responseView := req.GetResponseView()
	if responseView == cloudtaskspb.Task_VIEW_UNSPECIFIED {
		responseView = cloudtaskspb.Task_BASIC
	}

	res := &cloudtaskspb.ListTasksResponse{
		Tasks:         make([]*cloudtaskspb.Task, 0, len(dbTasks)),
		NextPageToken: "",
	}

	for _, t := range dbTasks {
		task := t.ToCloudTasksTask(responseView)
		// Override view based on request
		task.View = responseView
		res.Tasks = append(res.Tasks, task)
	}

	if hasMore {
		res.NextPageToken = strconv.Itoa(int(offset) + len(res.Tasks))
	}

	return res, nil
}

// Gets a task.
func (s *CloudTaskEmulator) GetTask(ctx context.Context, req *cloudtaskspb.GetTaskRequest) (*cloudtaskspb.Task, error) {
	// Validate task name
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task name is required")
	}
	if err := validateTaskName(req.Name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task name: %s", err.Error())
	}

	// Get task from database
	task, err := db.SelectTaskByName(ctx, s.mongoDB, req.Name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}

	// Convert to cloudtaskspb.Task
	view := req.GetResponseView()
	if view == cloudtaskspb.Task_VIEW_UNSPECIFIED {
		view = cloudtaskspb.Task_BASIC
	}
	result := task.ToCloudTasksTask(view)

	// Override view based on request if specified
	if req.ResponseView != cloudtaskspb.Task_VIEW_UNSPECIFIED {
		result.View = req.ResponseView
	} else if result.View == cloudtaskspb.Task_VIEW_UNSPECIFIED {
		// Default to BASIC if not set
		result.View = cloudtaskspb.Task_BASIC
	}

	return result, nil
}

// Creates a task and adds it to a queue.
//
// Tasks cannot be updated after creation; there is no UpdateTask command.
//
// * The maximum task size is 100KB.
func (s *CloudTaskEmulator) CreateTask(ctx context.Context, req *cloudtaskspb.CreateTaskRequest) (*cloudtaskspb.Task, error) {
	// Validate parent name (queue name)
	parentName := req.GetParent()
	if parentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "parent name is required")
	}
	if err := validateQueueName(parentName); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent name: %s", err.Error())
	}

	// Verify the queue exists and get its ID
	queue, err := db.SelectQueueByName(ctx, s.mongoDB, parentName)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "queue %s not found", parentName)
		}
		return nil, status.Errorf(codes.Internal, "failed to get queue: %v", err)
	}

	// Validate task is provided
	if req.Task == nil {
		return nil, status.Errorf(codes.InvalidArgument, "task is required")
	}

	// Validate or generate task name
	taskID := primitive.NewObjectID()
	taskName := req.Task.GetName()
	if taskName != "" {
		// Validate provided task name
		if err := validateTaskName(taskName); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid task name: %s", err.Error())
		}
		// Ensure task name is under the parent queue
		expectedPrefix := parentName + "/tasks/"
		if !strings.HasPrefix(taskName, expectedPrefix) {
			return nil, status.Errorf(codes.InvalidArgument, "task name must be under parent %s", parentName)
		}
	} else {
		// Generate task name if not provided
		taskName = fmt.Sprintf("%s/tasks/%s", parentName, taskID.Hex())
	}

	// Validate message type - only HttpRequest is supported
	if req.Task.GetAppEngineHttpRequest() != nil {
		return nil, status.Errorf(codes.InvalidArgument, "app engine http request is not supported")
	}
	httpRequest := req.Task.GetHttpRequest()
	if httpRequest == nil {
		return nil, status.Errorf(codes.InvalidArgument, "http_request is required")
	}

	// Validate URL
	if httpRequest.GetUrl() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "http_request.url is required")
	}
	if !strings.HasPrefix(httpRequest.GetUrl(), "http://") && !strings.HasPrefix(httpRequest.GetUrl(), "https://") {
		return nil, status.Errorf(codes.InvalidArgument, "http_request.url must begin with http:// or https://")
	}
	_, err = url.Parse(httpRequest.GetUrl())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "http_request.url is invalid: %s", err.Error())
	}

	// Validate dispatch deadline if provided (it's on the Task, not HttpRequest)
	var dispatchDeadline *time.Duration
	if req.Task.GetDispatchDeadline() != nil {
		deadline := req.Task.GetDispatchDeadline().AsDuration()
		// we don't enforce the lower bounds to allow for testing since this is just an emulator.
		if deadline < 0 || deadline > 30*time.Minute {
			return nil, status.Errorf(codes.InvalidArgument, "dispatch_deadline must be in the interval [15 seconds, 30 minutes]")
		}
		dispatchDeadline = &deadline
	}

	// Validate body is only set for POST, PUT, or PATCH
	if len(httpRequest.GetBody()) > 0 {
		method := httpRequest.GetHttpMethod()
		if method != cloudtaskspb.HttpMethod_POST && method != cloudtaskspb.HttpMethod_PUT && method != cloudtaskspb.HttpMethod_PATCH {
			return nil, status.Errorf(codes.InvalidArgument, "body can only be set for POST, PUT, or PATCH methods")
		}
	}

	// Validate task size (100KB limit)
	taskSize := len(httpRequest.GetBody())
	for k, v := range httpRequest.GetHeaders() {
		taskSize += len(k) + len(v)
	}
	if taskSize > 100*1024 {
		return nil, status.Errorf(codes.InvalidArgument, "task size exceeds maximum of 100KB")
	}

	// Handle schedule_time: if not set or in the past, set to current time
	now := time.Now()
	scheduleTime := now
	if req.Task.GetScheduleTime() != nil {
		scheduleTime = req.Task.GetScheduleTime().AsTime()
		if scheduleTime.Before(now) {
			scheduleTime = now
		}
	}

	// Create db.Task from cloudtaskspb.Task
	httpMethod := httpRequest.GetHttpMethod()
	if httpMethod == cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		httpMethod = cloudtaskspb.HttpMethod_POST // Default is POST per Cloud Tasks docs
	}

	dbTask := &db.Task{
		Id:        taskID,
		Name:      taskName,
		CreatedAt: now,
		UpdatedAt: now,
		Status:    db.TaskStatusPending,
		QueueID:   queue.Id,
		// from cloudtaskspb.Task
		MessageType: db.MessageTypeHttpRequest,
		HttpRequest: &db.Task_HttpRequest{
			Url:        httpRequest.GetUrl(),
			HttpMethod: httpMethod,
			Headers:    httpRequest.GetHeaders(),
			Body:       httpRequest.GetBody(),
		},
		ScheduleTime:     &scheduleTime,
		CreateTime:       &now,
		DispatchDeadline: dispatchDeadline,
	}

	if req.Task.GetDispatchDeadline() != nil {
		deadline := req.Task.GetDispatchDeadline().AsDuration()
		dbTask.DispatchDeadline = &deadline
	}

	// Set default values for output-only fields
	dbTask.DispatchCount = 0
	dbTask.ResponseCount = 0

	// Insert task into database
	dbTask, err = db.InsertTask(ctx, s.mongoDB, dbTask)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Task deduplication: if a task with the same name already exists
			return nil, status.Errorf(codes.AlreadyExists, "task with name %s already exists", taskName)
		}
		return nil, status.Errorf(codes.Internal, "failed to create task: %v", err)
	}

	// Convert back to cloudtaskspb.Task for response
	view := req.GetResponseView()
	if view == cloudtaskspb.Task_VIEW_UNSPECIFIED {
		view = cloudtaskspb.Task_BASIC
	}
	return dbTask.ToCloudTasksTask(view), nil
}

// Deletes a task.
//
// A task can be deleted if it is scheduled or dispatched. A task
// cannot be deleted if it has executed successfully or permanently
// failed.
func (s *CloudTaskEmulator) DeleteTask(ctx context.Context, req *cloudtaskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	// Validate task name
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task name is required")
	}
	if err := validateTaskName(req.Name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task name: %s", err.Error())
	}

	// Fetch task to check if it can be deleted
	task, err := db.SelectTaskByName(ctx, s.mongoDB, req.Name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}

	// Check if task has already succeeded or permanently failed
	// A task cannot be deleted if it has executed successfully or permanently failed
	if task.Status == db.TaskStatusSucceeded || task.Status == db.TaskStatusFailed {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s cannot be deleted because it has already succeeded or permanently failed", req.Name)
	}

	// Soft delete the task
	if err := db.SoftDeleteTaskByName(ctx, s.mongoDB, req.Name); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete task: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// Forces a task to run now.
//
// When this method is called, Cloud Tasks will dispatch the task, even if
// the task is already running, the queue has reached its
// [RateLimits][google.cloud.tasks.v2.RateLimits] or is
// [PAUSED][google.cloud.tasks.v2.Queue.State.PAUSED].
//
// This command is meant to be used for manual debugging. For
// example, [RunTask][google.cloud.tasks.v2.CloudTasks.RunTask] can be used to
// retry a failed task after a fix has been made or to manually force a task
// to be dispatched now.
//
// The dispatched task is returned. That is, the task that is returned
// contains the [status][Task.status] after the task is dispatched but
// before the task is received by its target.
//
// If Cloud Tasks receives a successful response from the task's
// target, then the task will be deleted; otherwise the task's
// [schedule_time][google.cloud.tasks.v2.Task.schedule_time] will be reset to
// the time that [RunTask][google.cloud.tasks.v2.CloudTasks.RunTask] was
// called plus the retry delay specified in the queue's
// [RetryConfig][google.cloud.tasks.v2.RetryConfig].
//
// [RunTask][google.cloud.tasks.v2.CloudTasks.RunTask] returns
// [NOT_FOUND][google.rpc.Code.NOT_FOUND] when it is called on a
// task that has already succeeded or permanently failed.
func (s *CloudTaskEmulator) RunTask(ctx context.Context, req *cloudtaskspb.RunTaskRequest) (*cloudtaskspb.Task, error) {
	// Validate task name
	if req.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "task name is required")
	}
	if err := validateTaskName(req.Name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task name: %s", err.Error())
	}

	// Fetch task from database
	task, err := db.SelectTaskByName(ctx, s.mongoDB, req.Name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}

	// Check if task has already succeeded or permanently failed
	if task.Status == db.TaskStatusSucceeded || task.Status == db.TaskStatusFailed {
		return nil, status.Errorf(codes.NotFound, "task %s has already succeeded or permanently failed", req.Name)
	}

	// Get the queue for this task
	queue, err := db.SelectQueueByID(ctx, s.mongoDB, task.QueueID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.Internal, "queue for task not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get queue: %v", err)
	}

	// Claim the task by setting status to Running and lock_expires_at
	// Reset schedule_time to now (as per Cloud Tasks API docs)
	// Note: RunTask is a debugging feature and may execute a task that is currently running
	now := time.Now()

	// Compute lock_expires_at from task.DispatchDeadline: now + deadline + 30s.
	// If no dispatch_deadline is set, use 10 minutes as a default HTTP deadline.
	leaseDuration := 10 * time.Minute
	if task.DispatchDeadline != nil {
		leaseDuration = *task.DispatchDeadline
	}
	lockDuration := leaseDuration + 30*time.Second
	lockExpiresAt := now.Add(lockDuration)

	col := s.mongoDB.Collection(db.CollectionTasks)
	updates := bson.M{
		"status":          db.TaskStatusRunning,
		"schedule_time":   now,
		"lock_expires_at": lockExpiresAt,
		"updated_at":      now,
	}
	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After)
	sr := col.FindOneAndUpdate(ctx, bson.M{"_id": task.Id}, bson.M{"$set": updates}, opts)
	claimedTask := &db.Task{}
	if err := sr.Decode(&claimedTask); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode task: %v", err)
	}

	go func(queue *db.Queue, t *db.Task, timeout time.Duration) {
		// Use a background or detached context so it outlives the RPC deadline,
		// but consider adding a reasonable timeout to avoid runaway work.
		bgCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := processTask(bgCtx, s.mongoDB, s.logger, queue, t); err != nil {
			s.logger.Error("RunTask background processing failed",
				zap.String("task_name", t.Name),
				zap.Error(err),
			)
		}
	}(queue, claimedTask, lockDuration)

	// Return task in state before running.
	// Convert to cloudtaskspb.Task
	view := cloudtaskspb.Task_BASIC
	if req.ResponseView != cloudtaskspb.Task_VIEW_UNSPECIFIED {
		view = req.ResponseView
	}
	return task.ToCloudTasksTask(view), nil
}

// validateTaskName validates that the task name follows the Cloud Tasks format:
// projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID
//
// PROJECT_ID can contain letters ([A-Za-z]), numbers ([0-9]), hyphens (-), colons (:), or periods (.)
// LOCATION_ID is the canonical ID for the task's location
// QUEUE_ID can contain letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Maximum length is 100 characters.
// TASK_ID can contain only letters ([A-Za-z]), numbers ([0-9]), hyphens (-), or underscores (_). Maximum length is 500 characters.
func validateTaskName(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	// PROJECT_ID: [A-Za-z0-9\-:.]
	projectIDPattern := `[A-Za-z0-9\-:.]+`
	// LOCATION_ID: typically lowercase alphanumeric with hyphens (e.g., "us-central1", "europe-west1")
	// We'll be lenient and allow alphanumeric, hyphens, and underscores
	locationIDPattern := `[A-Za-z0-9\-_]+`
	// QUEUE_ID: [A-Za-z0-9\-] with max length 100
	queueIDPattern := `[A-Za-z0-9\-]{1,100}`
	// TASK_ID: [A-Za-z0-9\-_] with max length 500
	taskIDPattern := `[A-Za-z0-9\-_]{1,500}`

	// Build the full regex pattern
	pattern := fmt.Sprintf(`^projects/(%s)/locations/(%s)/queues/(%s)/tasks/(%s)$`, projectIDPattern, locationIDPattern, queueIDPattern, taskIDPattern)
	re := regexp.MustCompile(pattern)

	if !re.MatchString(name) {
		return fmt.Errorf("task name must be in the format projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID, where PROJECT_ID can contain letters, numbers, hyphens, colons, or periods; LOCATION_ID is the canonical location ID; QUEUE_ID can contain letters, numbers, or hyphens (max 100 characters); and TASK_ID can contain letters, numbers, hyphens, or underscores (max 500 characters)")
	}

	// Extract and validate component lengths separately
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "queues" || parts[6] != "tasks" {
		return fmt.Errorf("task name must be in the format projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID")
	}

	queueID := parts[5]
	if len(queueID) > 100 {
		return fmt.Errorf("QUEUE_ID cannot exceed 100 characters")
	}

	taskID := parts[7]
	if len(taskID) > 500 {
		return fmt.Errorf("TASK_ID cannot exceed 500 characters")
	}

	return nil
}
