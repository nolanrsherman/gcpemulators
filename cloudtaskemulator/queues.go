package cloudtaskemulator

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Lists queues.
//
// Queues are returned in lexicographical order. Supports simple pagination via
// page_size and page_token. The filter field is currently ignored by the
// emulator.
func (s *CloudTaskEmulator) ListQueues(ctx context.Context, req *cloudtaskspb.ListQueuesRequest) (*cloudtaskspb.ListQueuesResponse, error) {
	// Validate parent
	if err := validateParentResourceName(req.GetParent()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent resource name: %v", err)
	}

	parentPrefix := req.GetParent() + "/queues/"

	// Determine page size (clamp to a reasonable maximum)
	pageSize := req.GetPageSize()
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 9800 {
		pageSize = 9800
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

	col := s.mongoDB.Collection(db.CollectionQueues)

	// Filter queues by parent prefix
	filter := bson.M{
		"name": bson.M{
			"$regex":   "^" + regexp.QuoteMeta(parentPrefix),
			"$options": "i",
		},
	}

	// Fetch one extra document to determine if there is a next page
	limit := int64(pageSize) + 1
	opts := options.Find().
		SetSort(bson.D{{Key: "name", Value: 1}}).
		SetSkip(offset).
		SetLimit(limit)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list queues: %v", err)
	}
	defer cursor.Close(ctx)

	var dbQueues []db.Queue
	if err := cursor.All(ctx, &dbQueues); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decode queues: %v", err)
	}

	hasMore := int32(len(dbQueues)) > pageSize
	if hasMore {
		dbQueues = dbQueues[:pageSize]
	}

	res := &cloudtaskspb.ListQueuesResponse{
		Queues:        make([]*cloudtaskspb.Queue, 0, len(dbQueues)),
		NextPageToken: "",
	}

	for _, q := range dbQueues {
		res.Queues = append(res.Queues, q.ToCloudTasksQueue())
	}

	if hasMore {
		res.NextPageToken = strconv.Itoa(int(offset) + len(res.Queues))
	}

	return res, nil
}

// Gets a queue.
func (s *CloudTaskEmulator) GetQueue(ctx context.Context, req *cloudtaskspb.GetQueueRequest) (*cloudtaskspb.Queue, error) {
	queue, err := db.SelectQueueByName(ctx, s.mongoDB, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get queue: %v", err)
	}
	return queue.ToCloudTasksQueue(), nil
}

// Creates a queue.
//
// Queues created with this method allow tasks to live for a maximum of 31
// days. After a task is 31 days old, the task will be deleted regardless of
// whether it was dispatched or not.
//
// WARNING: Using this method may have unintended side effects if you are
// using an App Engine `queue.yaml` or `queue.xml` file to manage your queues.
// Read
// [Overview of Queue Management and
// queue.yaml](https://cloud.google.com/tasks/docs/queue-yaml) before using
// this method.
func (s *CloudTaskEmulator) CreateQueue(ctx context.Context, req *cloudtaskspb.CreateQueueRequest) (*cloudtaskspb.Queue, error) {
	if err := validateParentResourceName(req.Parent); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent resource name: %v", err)
	}

	// If the client provided a queue name, validate it; otherwise we will generate one.
	if req.Queue.Name != "" {
		if err := validateQueueName(req.Queue.Name); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid queue name: %v", err)
		}
	}

	if req.Queue.AppEngineRoutingOverride != nil {
		return nil, status.Errorf(codes.InvalidArgument, "app engine routing override is not supported")
	}

	// Sensible retry defaults for the emulator:
	// - MaxRetryDuration: retry failed tasks for up to 10 minutes from first attempt
	// - MinBackoff: start with a 1 second backoff
	// - MaxBackoff: cap backoff at 1 minute between attempts
	defaultMaxRetryDuration := 10 * time.Minute
	defaultMinBackoff := 1 * time.Second
	defaultMaxBackoff := 1 * time.Minute

	queue := &db.Queue{
		Id:        primitive.NewObjectID(),
		Name:      req.Queue.Name,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),

		// Rate limits defaults (see db.Queue docs)
		MaxDispatchesPerSecond:  100, // reasonable default throughput
		MaxBurstSize:            25,
		MaxConcurrentDispatches: 50,

		// Retry config defaults
		MaxAttempts:      3,
		MaxRetryDuration: &defaultMaxRetryDuration,
		MinBackoff:       &defaultMinBackoff,
		MaxBackoff:       &defaultMaxBackoff,
		MaxDoublings:     3,

		PurgeTime: nil,
		State:     cloudtaskspb.Queue_RUNNING,
	}

	// If no name was provided, generate one under the parent resource.
	if queue.Name == "" {
		queue.Name = fmt.Sprintf("%s/queues/%s", req.Parent, queue.Id.Hex())
	} else {
		// Ensure the provided name is consistent with the parent.
		if !strings.HasPrefix(queue.Name, req.Parent+"/queues/") {
			return nil, status.Errorf(codes.InvalidArgument, "queue name must be under parent %s", req.Parent)
		}
	}

	// Apply user-defined rate limits if provided, validating against documented constraints.
	if req.Queue.RateLimits != nil {
		if req.Queue.RateLimits.MaxDispatchesPerSecond <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "max dispatches per second must be greater than 0")
		}
		if req.Queue.RateLimits.MaxDispatchesPerSecond > 500 {
			return nil, status.Errorf(codes.InvalidArgument, "max dispatches per second must be less than or equal to 500")
		}
		if req.Queue.RateLimits.MaxConcurrentDispatches <= 0 || req.Queue.RateLimits.MaxConcurrentDispatches > 5000 {
			return nil, status.Errorf(codes.InvalidArgument, "max concurrent dispatches must be between 1 and 5000")
		}

		queue.MaxDispatchesPerSecond = req.Queue.RateLimits.MaxDispatchesPerSecond
		queue.MaxConcurrentDispatches = req.Queue.RateLimits.MaxConcurrentDispatches

		// max_burst_size is output-only in Cloud Tasks and is derived from
		// max_dispatches_per_second. We ignore any user-provided value and
		// approximate the Cloud Tasks behavior with a simple function here.
		// This keeps the emulator behavior reasonable without exposing the
		// internal calculation details.
		if queue.MaxDispatchesPerSecond <= 10 {
			queue.MaxBurstSize = 10
		} else if queue.MaxDispatchesPerSecond <= 100 {
			queue.MaxBurstSize = 20
		} else {
			queue.MaxBurstSize = 25
		}
	}

	// Apply user-defined retry config if provided, validating bounds from db.Queue docs.
	if req.Queue.RetryConfig != nil {
		// MaxAttempts must be >= -1 (-1 means unlimited attempts)
		if req.Queue.RetryConfig.MaxAttempts < -1 {
			return nil, status.Errorf(codes.InvalidArgument, "max attempts must be greater than or equal to -1")
		}
		if req.Queue.RetryConfig.MaxRetryDuration != nil && req.Queue.RetryConfig.MaxRetryDuration.AsDuration() < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "max retry duration must be non-negative")
		}
		if req.Queue.RetryConfig.MinBackoff != nil && req.Queue.RetryConfig.MinBackoff.AsDuration() < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "min backoff must be non-negative")
		}
		if req.Queue.RetryConfig.MaxBackoff != nil && req.Queue.RetryConfig.MaxBackoff.AsDuration() < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "max backoff must be non-negative")
		}
		if req.Queue.RetryConfig.MinBackoff != nil && req.Queue.RetryConfig.MaxBackoff != nil &&
			req.Queue.RetryConfig.MaxBackoff.AsDuration() < req.Queue.RetryConfig.MinBackoff.AsDuration() {
			return nil, status.Errorf(codes.InvalidArgument, "max backoff must be greater than or equal to min backoff")
		}
		if req.Queue.RetryConfig.MaxDoublings < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "max doublings must be greater than or equal to 0")
		}

		queue.MaxAttempts = req.Queue.RetryConfig.MaxAttempts
		if req.Queue.RetryConfig.MaxRetryDuration != nil {
			d := req.Queue.RetryConfig.MaxRetryDuration.AsDuration()
			queue.MaxRetryDuration = &d
		}
		if req.Queue.RetryConfig.MinBackoff != nil {
			d := req.Queue.RetryConfig.MinBackoff.AsDuration()
			queue.MinBackoff = &d
		}
		if req.Queue.RetryConfig.MaxBackoff != nil {
			d := req.Queue.RetryConfig.MaxBackoff.AsDuration()
			queue.MaxBackoff = &d
		}
		queue.MaxDoublings = req.Queue.RetryConfig.MaxDoublings
	}

	queue, err := db.InsertQueue(ctx, s.mongoDB, queue)
	if err != nil {
		return nil, err
	}

	return queue.ToCloudTasksQueue(), nil
}

// Updates a queue.
//
// This method creates the queue if it does not exist and updates
// the queue if it does exist.
//
// Queues created with this method allow tasks to live for a maximum of 31
// days. After a task is 31 days old, the task will be deleted regardless of
// whether it was dispatched or not.
//
// WARNING: Using this method may have unintended side effects if you are
// using an App Engine `queue.yaml` or `queue.xml` file to manage your queues.
// Read
// [Overview of Queue Management and
// queue.yaml](https://cloud.google.com/tasks/docs/queue-yaml) before using
// this method.
func (s *CloudTaskEmulator) UpdateQueue(ctx context.Context, req *cloudtaskspb.UpdateQueueRequest) (*cloudtaskspb.Queue, error) {
	if req.GetQueue() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "queue must be provided")
	}

	q := req.GetQueue()

	name := q.GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "queue name is required")
	}

	// Validate queue name and parent.
	if err := validateQueueName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name: %v", err)
	}
	parent := strings.Join(strings.Split(name, "/")[:4], "/") // projects/{id}/locations/{id}
	if err := validateParentResourceName(parent); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent resource name: %v", err)
	}

	// Determine which fields are being updated based on the update mask.
	// If the mask is empty or nil, then all mutable fields are updated.
	updateMask := req.GetUpdateMask()
	updateAll := updateMask == nil || len(updateMask.Paths) == 0
	updateRateLimits := false
	updateRetryConfig := false

	if !updateAll {
		for _, path := range updateMask.Paths {
			switch path {
			case "rate_limits":
				updateRateLimits = true
			case "retry_config":
				updateRetryConfig = true
			case "name":
				// Name is required in the request but is not updatable.
				return nil, status.Errorf(codes.InvalidArgument, "name cannot be updated via UpdateQueue")
			default:
				return nil, status.Errorf(codes.InvalidArgument, "unsupported update mask path: %s", path)
			}
		}
	} else {
		updateRateLimits = true
		updateRetryConfig = true
	}

	// Output-only / unsupported fields must not be updated.
	if q.AppEngineRoutingOverride != nil {
		return nil, status.Errorf(codes.InvalidArgument, "AppEngineRoutingOverride cannot be updated via UpdateQueue")
	}
	if q.State != cloudtaskspb.Queue_STATE_UNSPECIFIED {
		return nil, status.Errorf(codes.InvalidArgument, "state cannot be updated via UpdateQueue")
	}
	if q.PurgeTime != nil {
		return nil, status.Errorf(codes.InvalidArgument, "purge_time cannot be updated via UpdateQueue")
	}
	if q.StackdriverLoggingConfig != nil {
		return nil, status.Errorf(codes.InvalidArgument, "stackdriver_logging_config cannot be updated via UpdateQueue")
	}

	// Fetch existing queue, if any.
	existing, err := db.SelectQueueByName(ctx, s.mongoDB, name)
	if err != nil {
		// If not found, create a new queue using the same defaults as CreateQueue.
		existing = &db.Queue{
			Id:        primitive.NewObjectID(),
			Name:      name,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			// Rate limits defaults
			MaxDispatchesPerSecond:  100,
			MaxBurstSize:            25,
			MaxConcurrentDispatches: 50,
			// Retry config defaults
			MaxAttempts: 3,
			// set below for clarity
			MaxDoublings: 3,
			PurgeTime:    nil,
			State:        cloudtaskspb.Queue_RUNNING,
		}
		// Initialize default retry durations
		defMaxRetry := 10 * time.Minute
		defMinBackoff := 1 * time.Second
		defMaxBackoff := 1 * time.Minute
		existing.MaxRetryDuration = &defMaxRetry
		existing.MinBackoff = &defMinBackoff
		existing.MaxBackoff = &defMaxBackoff
	}

	// Apply RateLimits if requested.
	if updateAll || updateRateLimits {
		rl := q.GetRateLimits()
		if rl == nil {
			// If the mask explicitly targets rate_limits but the field is unset,
			// we leave existing values as-is. Callers should provide a value
			// when they include a field in the update mask.
		} else {
			if rl.MaxDispatchesPerSecond <= 0 {
				return nil, status.Errorf(codes.InvalidArgument, "max dispatches per second must be greater than 0")
			}
			if rl.MaxDispatchesPerSecond > 500 {
				return nil, status.Errorf(codes.InvalidArgument, "max dispatches per second must be less than or equal to 500")
			}
			if rl.MaxConcurrentDispatches <= 0 || rl.MaxConcurrentDispatches > 5000 {
				return nil, status.Errorf(codes.InvalidArgument, "max concurrent dispatches must be between 1 and 5000")
			}
			existing.MaxDispatchesPerSecond = rl.MaxDispatchesPerSecond
			existing.MaxConcurrentDispatches = rl.MaxConcurrentDispatches

			// Derive MaxBurstSize from MaxDispatchesPerSecond.
			if existing.MaxDispatchesPerSecond <= 10 {
				existing.MaxBurstSize = 10
			} else if existing.MaxDispatchesPerSecond <= 100 {
				existing.MaxBurstSize = 20
			} else {
				existing.MaxBurstSize = 25
			}
		}
	}

	// Apply RetryConfig if requested.
	if updateAll || updateRetryConfig {
		rc := q.GetRetryConfig()
		if rc == nil {
			// If the mask explicitly targets retry_config but the field is unset,
			// we leave existing values as-is. Callers should provide a value
			// when they include a field in the update mask.
		} else {
			if rc.MaxAttempts < -1 {
				return nil, status.Errorf(codes.InvalidArgument, "max attempts must be greater than or equal to -1")
			}
			if rc.MaxRetryDuration != nil && rc.MaxRetryDuration.AsDuration() < 0 {
				return nil, status.Errorf(codes.InvalidArgument, "max retry duration must be non-negative")
			}
			if rc.MinBackoff != nil && rc.MinBackoff.AsDuration() < 0 {
				return nil, status.Errorf(codes.InvalidArgument, "min backoff must be non-negative")
			}
			if rc.MaxBackoff != nil && rc.MaxBackoff.AsDuration() < 0 {
				return nil, status.Errorf(codes.InvalidArgument, "max backoff must be non-negative")
			}
			if rc.MinBackoff != nil && rc.MaxBackoff != nil &&
				rc.MaxBackoff.AsDuration() < rc.MinBackoff.AsDuration() {
				return nil, status.Errorf(codes.InvalidArgument, "max backoff must be greater than or equal to min backoff")
			}
			if rc.MaxDoublings < 0 {
				return nil, status.Errorf(codes.InvalidArgument, "max doublings must be greater than or equal to 0")
			}

			existing.MaxAttempts = rc.MaxAttempts
			if rc.MaxRetryDuration != nil {
				d := rc.MaxRetryDuration.AsDuration()
				existing.MaxRetryDuration = &d
			}
			if rc.MinBackoff != nil {
				d := rc.MinBackoff.AsDuration()
				existing.MinBackoff = &d
			}
			if rc.MaxBackoff != nil {
				d := rc.MaxBackoff.AsDuration()
				existing.MaxBackoff = &d
			}
			existing.MaxDoublings = rc.MaxDoublings
		}
	}

	existing.UpdatedAt = time.Now()

	// Upsert queue by name.
	col := s.mongoDB.Collection(db.CollectionQueues)
	if _, err := col.UpdateOne(
		ctx,
		bson.M{"name": existing.Name},
		bson.M{"$set": existing},
		options.Update().SetUpsert(true),
	); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update queue: %v", err)
	}

	return existing.ToCloudTasksQueue(), nil
}

// Deletes a queue.
//
// This command will mark the queue as deleted; a TTL index will remove it
// permanently from storage after a retention period. Until then, normal
// queries will ignore soft-deleted queues.
//
// Note: If you delete a queue, a queue with the same name can't be created
// for 7 days.
//
// WARNING: Using this method may have unintended side effects if you are
// using an App Engine `queue.yaml` or `queue.xml` file to manage your queues.
// Read
// [Overview of Queue Management and
// queue.yaml](https://cloud.google.com/tasks/docs/queue-yaml) before using
// this method.
func (s *CloudTaskEmulator) DeleteQueue(ctx context.Context, req *cloudtaskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "queue name is required")
	}
	if err := validateQueueName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name: %v", err)
	}

	if err := db.SoftDeleteQueueByName(ctx, s.mongoDB, name); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "queue %s not found", name)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete queue: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// Purges a queue by deleting all of its tasks.
//
// All tasks created before this method is called are permanently deleted.
//
// Purge operations can take up to one minute to take effect. Tasks
// might be dispatched before the purge takes effect. A purge is irreversible.
func (s *CloudTaskEmulator) PurgeQueue(context.Context, *cloudtaskspb.PurgeQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PurgeQueue not implemented")
}

// Pauses the queue.
//
// If a queue is paused then the system will stop dispatching tasks
// until the queue is resumed via
// [ResumeQueue][google.cloud.tasks.v2.CloudTasks.ResumeQueue]. Tasks can
// still be added when the queue is paused. A queue is paused if its
// [state][google.cloud.tasks.v2.Queue.state] is
// [PAUSED][google.cloud.tasks.v2.Queue.State.PAUSED].
func (s *CloudTaskEmulator) PauseQueue(ctx context.Context, req *cloudtaskspb.PauseQueueRequest) (*cloudtaskspb.Queue, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "queue name is required")
	}
	if err := validateQueueName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name: %v", err)
	}

	if err := db.UpdateQueueState(ctx, s.mongoDB, name, cloudtaskspb.Queue_PAUSED); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "queue %s not found", name)
		}
		return nil, status.Errorf(codes.Internal, "failed to pause queue: %v", err)
	}

	// Fetch and return the updated queue
	queue, err := db.SelectQueueByName(ctx, s.mongoDB, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get updated queue: %v", err)
	}

	return queue.ToCloudTasksQueue(), nil
}

// Resume a queue.
//
// This method resumes a queue after it has been
// [PAUSED][google.cloud.tasks.v2.Queue.State.PAUSED] or
// [DISABLED][google.cloud.tasks.v2.Queue.State.DISABLED]. The state of a
// queue is stored in the queue's [state][google.cloud.tasks.v2.Queue.state];
// after calling this method it will be set to
// [RUNNING][google.cloud.tasks.v2.Queue.State.RUNNING].
//
// WARNING: Resuming many high-QPS queues at the same time can
// lead to target overloading. If you are resuming high-QPS
// queues, follow the 500/50/5 pattern described in
// [Managing Cloud Tasks Scaling
// Risks](https://cloud.google.com/tasks/docs/manage-cloud-task-scaling).
func (s *CloudTaskEmulator) ResumeQueue(ctx context.Context, req *cloudtaskspb.ResumeQueueRequest) (*cloudtaskspb.Queue, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "queue name is required")
	}
	if err := validateQueueName(name); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid queue name: %v", err)
	}

	if err := db.UpdateQueueState(ctx, s.mongoDB, name, cloudtaskspb.Queue_RUNNING); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, status.Errorf(codes.NotFound, "queue %s not found", name)
		}
		return nil, status.Errorf(codes.Internal, "failed to resume queue: %v", err)
	}

	// Fetch and return the updated queue
	queue, err := db.SelectQueueByName(ctx, s.mongoDB, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get updated queue: %v", err)
	}

	return queue.ToCloudTasksQueue(), nil
}

// validateQueueName validates that the queue name follows the Cloud Tasks format:
// projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID
//
// PROJECT_ID can contain letters ([A-Za-z]), numbers ([0-9]), hyphens (-), colons (:), or periods (.)
// LOCATION_ID is the canonical ID for the queue's location
// QUEUE_ID can contain letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Maximum length is 100 characters.
func validateQueueName(name string) error {
	if name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	// PROJECT_ID: [A-Za-z0-9\-:.]
	projectIDPattern := `[A-Za-z0-9\-:.]+`
	// LOCATION_ID: typically lowercase alphanumeric with hyphens (e.g., "us-central1", "europe-west1")
	// We'll be lenient and allow alphanumeric, hyphens, and underscores
	locationIDPattern := `[A-Za-z0-9\-_]+`
	// QUEUE_ID: [A-Za-z0-9\-] with max length 100
	queueIDPattern := `[A-Za-z0-9\-]{1,100}`

	// Build the full regex pattern
	pattern := fmt.Sprintf(`^projects/(%s)/locations/(%s)/queues/(%s)$`, projectIDPattern, locationIDPattern, queueIDPattern)
	re := regexp.MustCompile(pattern)

	if !re.MatchString(name) {
		return fmt.Errorf("queue name must be in the format projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID, where PROJECT_ID can contain letters, numbers, hyphens, colons, or periods; LOCATION_ID is the canonical location ID; and QUEUE_ID can contain letters, numbers, or hyphens (max 100 characters)")
	}

	// Extract and validate QUEUE_ID length separately (regex already enforces max 100, but let's be explicit)
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "queues" {
		return fmt.Errorf("queue name must be in the format projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID")
	}

	queueID := parts[5]
	if len(queueID) > 100 {
		return fmt.Errorf("QUEUE_ID cannot exceed 100 characters")
	}

	return nil
}

// validateQueueName validates that the queue name follows the Cloud Tasks format:
// projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID
//
// PROJECT_ID can contain letters ([A-Za-z]), numbers ([0-9]), hyphens (-), colons (:), or periods (.)
// LOCATION_ID is the canonical ID for the queue's location
// QUEUE_ID can contain letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Maximum length is 100 characters.
func validateParentResourceName(name string) error {
	if name == "" {
		return fmt.Errorf("parent resource name cannot be empty")
	}

	// PROJECT_ID: [A-Za-z0-9\-:.]
	projectIDPattern := `[A-Za-z0-9\-:.]+`
	// LOCATION_ID: typically lowercase alphanumeric with hyphens (e.g., "us-central1", "europe-west1")
	// We'll be lenient and allow alphanumeric, hyphens, and underscores
	locationIDPattern := `[A-Za-z0-9\-_]+`

	// Build the full regex pattern
	pattern := fmt.Sprintf(`^projects/(%s)/locations/(%s)$`, projectIDPattern, locationIDPattern)
	re := regexp.MustCompile(pattern)

	if !re.MatchString(name) {
		return fmt.Errorf("parent resource name must be in the format projects/PROJECT_ID/locations/LOCATION_ID, where PROJECT_ID can contain letters, numbers, hyphens, colons, or periods; LOCATION_ID is the canonical location ID")
	}

	// Extract and validate QUEUE_ID length separately (regex already enforces max 100, but let's be explicit)
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "locations" {
		return fmt.Errorf("queue name must be in the format projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID")
	}

	return nil
}
