package cloudtaskemulator

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
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
func (s *Server) ListTasks(context.Context, *cloudtaskspb.ListTasksRequest) (*cloudtaskspb.ListTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListTasks not implemented")
}

// Gets a task.
func (s *Server) GetTask(context.Context, *cloudtaskspb.GetTaskRequest) (*cloudtaskspb.Task, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTask not implemented")
}

// Creates a task and adds it to a queue.
//
// Tasks cannot be updated after creation; there is no UpdateTask command.
//
// * The maximum task size is 100KB.
func (s *Server) CreateTask(ctx context.Context, req *cloudtaskspb.CreateTaskRequest) (*cloudtaskspb.Task, error) {
	// validate parent name and extract queue id
	parentName := req.GetParent()
	if parentName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "parent name is required")
	}
	err := validateQueueName(parentName)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent name: %s", err.Error())
	}
	// queueId := strings.Split(parentName, "/queues/")[1]

	return nil, status.Errorf(codes.Unimplemented, "method CreateTask not implemented")
}

// Deletes a task.
//
// A task can be deleted if it is scheduled or dispatched. A task
// cannot be deleted if it has executed successfully or permanently
// failed.
func (s *Server) DeleteTask(context.Context, *cloudtaskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteTask not implemented")
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
func (s *Server) RunTask(context.Context, *cloudtaskspb.RunTaskRequest) (*cloudtaskspb.Task, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunTask not implemented")
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
