// We intentionally do not store the raw protobuf messages directly because some
// fields (like cloudtaskspb.Task_MessageType cannot be automatically unmarshaled by MongoDB.
// Instead of introducing custom BSON codecs, we model only the fields we need
// for the Cloud Tasks emulator so decoding remains simple and explicit.
package db

import (
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Queue struct {
	Id        primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	CreatedAt time.Time          `bson:"created_at,omitempty" json:"created_at,omitempty"`
	UpdatedAt time.Time          `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
	// When set, the queue has been soft-deleted and will be removed by a TTL
	// index a few days later. Active queries must ignore documents with this
	// field set.
	DeletedAt *time.Time `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
	// Caller-specified and required in
	// [CreateQueue][google.cloud.tasks.v2.CloudTasks.CreateQueue], after which it
	// becomes output only.
	//
	// The queue name.
	//
	// The queue name must have the following format:
	// `projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID`
	//
	//   - `PROJECT_ID` can contain letters ([A-Za-z]), numbers ([0-9]),
	//     hyphens (-), colons (:), or periods (.).
	//     For more information, see
	//     [Identifying
	//     projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects#identifying_projects)
	//   - `LOCATION_ID` is the canonical ID for the queue's location.
	//     The list of available locations can be obtained by calling
	//     [ListLocations][google.cloud.location.Locations.ListLocations].
	//     For more information, see https://cloud.google.com/about/locations/.
	//   - `QUEUE_ID` can contain letters ([A-Za-z]), numbers ([0-9]), or
	//     hyphens (-). The maximum length is 100 characters.
	Name string `json:"name,omitempty"`
	// The maximum rate at which tasks are dispatched from this queue.
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// * The maximum allowed value is 500.
	//
	// This field has the same meaning as
	// [rate in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#rate).
	MaxDispatchesPerSecond float64 `json:"max_dispatches_per_second,omitempty"`
	// Output only. The max burst size.
	//
	// Max burst size limits how fast tasks in queue are processed when
	// many tasks are in the queue and the rate is high. This field
	// allows the queue to have a high rate so processing starts shortly
	// after a task is enqueued, but still limits resource usage when
	// many tasks are enqueued in a short period of time.
	//
	// The [token bucket](https://wikipedia.org/wiki/Token_Bucket)
	// algorithm is used to control the rate of task dispatches. Each
	// queue has a token bucket that holds tokens, up to the maximum
	// specified by `max_burst_size`. Each time a task is dispatched, a
	// token is removed from the bucket. Tasks will be dispatched until
	// the queue's bucket runs out of tokens. The bucket will be
	// continuously refilled with new tokens based on
	// [max_dispatches_per_second][google.cloud.tasks.v2.RateLimits.max_dispatches_per_second].
	//
	// Cloud Tasks will pick the value of `max_burst_size` based on the
	// value of
	// [max_dispatches_per_second][google.cloud.tasks.v2.RateLimits.max_dispatches_per_second].
	//
	// For queues that were created or updated using
	// `queue.yaml/xml`, `max_burst_size` is equal to
	// [bucket_size](https://cloud.google.com/appengine/docs/standard/python/config/queueref#bucket_size).
	// Since `max_burst_size` is output only, if
	// [UpdateQueue][google.cloud.tasks.v2.CloudTasks.UpdateQueue] is called on a
	// queue created by `queue.yaml/xml`, `max_burst_size` will be reset based on
	// the value of
	// [max_dispatches_per_second][google.cloud.tasks.v2.RateLimits.max_dispatches_per_second],
	// regardless of whether
	// [max_dispatches_per_second][google.cloud.tasks.v2.RateLimits.max_dispatches_per_second]
	// is updated.
	MaxBurstSize int32 `json:"max_burst_size,omitempty"`
	// The maximum number of concurrent tasks that Cloud Tasks allows
	// to be dispatched for this queue. After this threshold has been
	// reached, Cloud Tasks stops dispatching tasks until the number of
	// concurrent requests decreases.
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// The maximum allowed value is 5,000.
	//
	// This field has the same meaning as
	// [max_concurrent_requests in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#max_concurrent_requests).
	MaxConcurrentDispatches int32 `json:"max_concurrent_dispatches,omitempty"`
	// Number of attempts per task.
	//
	// Cloud Tasks will attempt the task `max_attempts` times (that is, if the
	// first attempt fails, then there will be `max_attempts - 1` retries). Must
	// be >= -1.
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// -1 indicates unlimited attempts.
	//
	// This field has the same meaning as
	// [task_retry_limit in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#retry_parameters).
	MaxAttempts int32 `json:"max_attempts,omitempty"`
	// If positive, `max_retry_duration` specifies the time limit for
	// retrying a failed task, measured from when the task was first
	// attempted. Once `max_retry_duration` time has passed *and* the
	// task has been attempted
	// [max_attempts][google.cloud.tasks.v2.RetryConfig.max_attempts] times, no
	// further attempts will be made and the task will be deleted.
	//
	// If zero, then the task age is unlimited.
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// `max_retry_duration` will be truncated to the nearest second.
	//
	// This field has the same meaning as
	// [task_age_limit in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#retry_parameters).
	MaxRetryDuration *time.Duration `json:"max_retry_duration,omitempty"`
	// A task will be [scheduled][google.cloud.tasks.v2.Task.schedule_time] for
	// retry between [min_backoff][google.cloud.tasks.v2.RetryConfig.min_backoff]
	// and [max_backoff][google.cloud.tasks.v2.RetryConfig.max_backoff] duration
	// after it fails, if the queue's
	// [RetryConfig][google.cloud.tasks.v2.RetryConfig] specifies that the task
	// should be retried.
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// `min_backoff` will be truncated to the nearest second.
	//
	// This field has the same meaning as
	// [min_backoff_seconds in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#retry_parameters).
	MinBackoff *time.Duration `json:"min_backoff,omitempty"`
	// A task will be [scheduled][google.cloud.tasks.v2.Task.schedule_time] for
	// retry between [min_backoff][google.cloud.tasks.v2.RetryConfig.min_backoff]
	// and [max_backoff][google.cloud.tasks.v2.RetryConfig.max_backoff] duration
	// after it fails, if the queue's
	// [RetryConfig][google.cloud.tasks.v2.RetryConfig] specifies that the task
	// should be retried.
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// `max_backoff` will be truncated to the nearest second.
	//
	// This field has the same meaning as
	// [max_backoff_seconds in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#retry_parameters).
	MaxBackoff *time.Duration `json:"max_backoff,omitempty"`
	// The time between retries will double `max_doublings` times.
	//
	// A task's retry interval starts at
	// [min_backoff][google.cloud.tasks.v2.RetryConfig.min_backoff], then doubles
	// `max_doublings` times, then increases linearly, and finally
	// retries at intervals of
	// [max_backoff][google.cloud.tasks.v2.RetryConfig.max_backoff] up to
	// [max_attempts][google.cloud.tasks.v2.RetryConfig.max_attempts] times.
	//
	// For example, if
	// [min_backoff][google.cloud.tasks.v2.RetryConfig.min_backoff] is 10s,
	// [max_backoff][google.cloud.tasks.v2.RetryConfig.max_backoff] is 300s, and
	// `max_doublings` is 3, then the a task will first be retried in
	// 10s. The retry interval will double three times, and then
	// increase linearly by 2^3 * 10s.  Finally, the task will retry at
	// intervals of [max_backoff][google.cloud.tasks.v2.RetryConfig.max_backoff]
	// until the task has been attempted
	// [max_attempts][google.cloud.tasks.v2.RetryConfig.max_attempts] times. Thus,
	// the requests will retry at 10s, 20s, 40s, 80s, 160s, 240s, 300s, 300s, ....
	//
	// If unspecified when the queue is created, Cloud Tasks will pick the
	// default.
	//
	// This field has the same meaning as
	// [max_doublings in
	// queue.yaml/xml](https://cloud.google.com/appengine/docs/standard/python/config/queueref#retry_parameters).
	MaxDoublings int32 `json:"max_doublings,omitempty"`
	// Output only. The state of the queue.
	//
	// `state` can only be changed by calling
	// [PauseQueue][google.cloud.tasks.v2.CloudTasks.PauseQueue],
	// [ResumeQueue][google.cloud.tasks.v2.CloudTasks.ResumeQueue], or uploading
	// [queue.yaml/xml](https://cloud.google.com/appengine/docs/python/config/queueref).
	// [UpdateQueue][google.cloud.tasks.v2.CloudTasks.UpdateQueue] cannot be used
	// to change `state`.
	State cloudtaskspb.Queue_State `json:"state,omitempty"`
	// Output only. The last time this queue was purged.
	//
	// All tasks that were [created][google.cloud.tasks.v2.Task.create_time]
	// before this time were purged.
	//
	// A queue can be purged using
	// [PurgeQueue][google.cloud.tasks.v2.CloudTasks.PurgeQueue], the [App Engine
	// Task Queue SDK, or the Cloud
	// Console](https://cloud.google.com/appengine/docs/standard/python/taskqueue/push/deleting-tasks-and-queues#purging_all_tasks_from_a_queue).
	//
	// Purge time will be truncated to the nearest microsecond. Purge
	// time will be unset if the queue has never been purged.
	PurgeTime *time.Time `json:"purge_time,omitempty"`
}

func (q *Queue) ToCloudTasksQueue() *cloudtaskspb.Queue {
	var purgeTime *timestamppb.Timestamp
	if q.PurgeTime != nil {
		purgeTime = timestamppb.New(*q.PurgeTime)
	}
	var maxRetryDuration *durationpb.Duration
	if q.MaxRetryDuration != nil {
		maxRetryDuration = durationpb.New(*q.MaxRetryDuration)
	}
	var minBackoff *durationpb.Duration
	if q.MinBackoff != nil {
		minBackoff = durationpb.New(*q.MinBackoff)
	}
	var maxBackoff *durationpb.Duration
	if q.MaxBackoff != nil {
		maxBackoff = durationpb.New(*q.MaxBackoff)
	}
	return &cloudtaskspb.Queue{
		Name:      q.Name,
		State:     q.State,
		PurgeTime: purgeTime,
		// not supported
		AppEngineRoutingOverride: nil,
		RateLimits: &cloudtaskspb.RateLimits{
			MaxDispatchesPerSecond:  q.MaxDispatchesPerSecond,
			MaxBurstSize:            q.MaxBurstSize,
			MaxConcurrentDispatches: q.MaxConcurrentDispatches,
		},
		RetryConfig: &cloudtaskspb.RetryConfig{
			MaxAttempts:      q.MaxAttempts,
			MaxRetryDuration: maxRetryDuration,
			MinBackoff:       minBackoff,
			MaxBackoff:       maxBackoff,
			MaxDoublings:     q.MaxDoublings,
		},
	}
}

type TaskStatus int

const (
	TaskStatusPending   TaskStatus = 0
	TaskStatusRunning   TaskStatus = 1
	TaskStatusSucceeded TaskStatus = 2
	TaskStatusFailed    TaskStatus = 3
)

const (
	MessageTypeHttpRequest          = "Task_HttpRequest"
	MessageTypeAppEngineHttpRequest = "Task_AppEngineHttpRequest"
)

type Task struct {
	Id        primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	CreatedAt time.Time          `bson:"created_at,omitempty" json:"created_at,omitempty"`
	UpdatedAt time.Time          `bson:"updated_at,omitempty" json:"updated_at,omitempty"`
	DeletedAt *time.Time         `bson:"deleted_at,omitempty" json:"deleted_at,omitempty"`
	Status    TaskStatus         `bson:"status,omitempty" json:"status,omitempty"`
	// QueueID is the ObjectID of the queue this task belongs to.
	// Used for efficient querying and indexing.
	QueueID primitive.ObjectID `bson:"queue_id,omitempty" json:"queue_id,omitempty"`
	// LockExpiresAt is when the worker's lease expires (prevents stuck tasks).
	// If task is "running" and LockExpiresAt < now, task can be reclaimed.
	// Set to: now + lease_duration when task is claimed.
	LockExpiresAt *time.Time `bson:"lock_expires_at,omitempty" json:"lock_expires_at,omitempty"`
	// from cloudtaskspb.Task
	// Optionally caller-specified in
	// [CreateTask][google.cloud.tasks.v2.CloudTasks.CreateTask].
	//
	// The task name.
	//
	// The task name must have the following format:
	// `projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID`
	//
	//   - `PROJECT_ID` can contain letters ([A-Za-z]), numbers ([0-9]),
	//     hyphens (-), colons (:), or periods (.).
	//     For more information, see
	//     [Identifying
	//     projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects#identifying_projects)
	//   - `LOCATION_ID` is the canonical ID for the task's location.
	//     The list of available locations can be obtained by calling
	//     [ListLocations][google.cloud.location.Locations.ListLocations].
	//     For more information, see https://cloud.google.com/about/locations/.
	//   - `QUEUE_ID` can contain letters ([A-Za-z]), numbers ([0-9]), or
	//     hyphens (-). The maximum length is 100 characters.
	//   - `TASK_ID` can contain only letters ([A-Za-z]), numbers ([0-9]),
	//     hyphens (-), or underscores (_). The maximum length is 500 characters.
	Name string `json:"name,omitempty"`
	// Required. The message to send to the worker.
	//
	// Types that are assignable to MessageType:
	//
	//	"Task_AppEngineHttpRequest"
	//	"Task_HttpRequest"
	// We only support Task_HttpRequest tasks for now.
	MessageType string `json:"message_type,omitempty"`
	// Used when MessageType is "Task_HttpRequest". This is the only message type we support for now.
	HttpRequest *Task_HttpRequest `json:"http_request,omitempty"`
	// The time when the task is scheduled to be attempted or retried.
	//
	// `schedule_time` will be truncated to the nearest microsecond.
	ScheduleTime *time.Time `json:"schedule_time,omitempty"`
	// Output only. The time that the task was created.
	//
	// `create_time` will be truncated to the nearest second.
	CreateTime *time.Time `json:"create_time,omitempty"`
	// The deadline for requests sent to the worker. If the worker does not
	// respond by this deadline then the request is cancelled and the attempt
	// is marked as a `DEADLINE_EXCEEDED` failure. Cloud Tasks will retry the
	// task according to the [RetryConfig][google.cloud.tasks.v2.RetryConfig].
	//
	// Note that when the request is cancelled, Cloud Tasks will stop listening
	// for the response, but whether the worker stops processing depends on the
	// worker. For example, if the worker is stuck, it may not react to cancelled
	// requests.
	//
	// The default and maximum values depend on the type of request:
	//
	// * For [HTTP tasks][google.cloud.tasks.v2.HttpRequest], the default is 10
	// minutes. The deadline
	//
	//	must be in the interval [15 seconds, 30 minutes].
	//
	// * For [App Engine tasks][google.cloud.tasks.v2.AppEngineHttpRequest], 0
	// indicates that the
	//
	//	request has the default deadline. The default deadline depends on the
	//	[scaling
	//	type](https://cloud.google.com/appengine/docs/standard/go/how-instances-are-managed#instance_scaling)
	//	of the service: 10 minutes for standard apps with automatic scaling, 24
	//	hours for standard apps with manual and basic scaling, and 60 minutes for
	//	flex apps. If the request deadline is set, it must be in the interval [15
	//	seconds, 24 hours 15 seconds]. Regardless of the task's
	//	`dispatch_deadline`, the app handler will not run for longer than than
	//	the service's timeout. We recommend setting the `dispatch_deadline` to
	//	at most a few seconds more than the app handler's timeout. For more
	//	information see
	//	[Timeouts](https://cloud.google.com/tasks/docs/creating-appengine-handlers#timeouts).
	//
	// `dispatch_deadline` will be truncated to the nearest millisecond. The
	// deadline is an approximate deadline.
	DispatchDeadline *time.Duration `json:"dispatch_deadline,omitempty"`
	// Output only. The number of attempts dispatched.
	//
	// This count includes attempts which have been dispatched but haven't
	// received a response.
	DispatchCount int32 `protobuf:"varint,7,opt,name=dispatch_count,json=dispatchCount,proto3" json:"dispatch_count,omitempty"`
	// Output only. The number of attempts which have received a response.
	ResponseCount int32 `protobuf:"varint,8,opt,name=response_count,json=responseCount,proto3" json:"response_count,omitempty"`
	// Output only. The status of the task's first attempt.
	//
	// Only [dispatch_time][google.cloud.tasks.v2.Attempt.dispatch_time] will be
	// set. The other [Attempt][google.cloud.tasks.v2.Attempt] information is not
	// retained by Cloud Tasks.
	FirstAttempt *Task_Attempt `json:"first_attempt,omitempty"`

	// Output only. The status of the task's last attempt.
	LastAttempt *Task_Attempt `json:"last_attempt,omitempty"`
}

type Task_Attempt struct {
	// Output only. The time that this attempt was scheduled.
	//
	// `schedule_time` will be truncated to the nearest microsecond.
	ScheduleTime *time.Time `json:"schedule_time,omitempty"`
	// Output only. The time that this attempt was dispatched.
	//
	// `dispatch_time` will be truncated to the nearest microsecond.
	DispatchTime *time.Time `json:"dispatch_time,omitempty"`
	// Output only. The time that this attempt response was received.
	//
	// `response_time` will be truncated to the nearest microsecond.
	ResponseTime *time.Time `json:"response_time,omitempty"`
	// Output only. The response status code from the worker for this attempt.
	//
	// Mirrors the gRPC status code stored in cloudtaskspb.Attempt.ResponseStatus.Code.
	ResponseStatusCode int32 `json:"response_status_code,omitempty"`
	// Output only. The response status message from the worker for this attempt.
	//
	// Mirrors cloudtaskspb.Attempt.ResponseStatus.Message.
	ResponseStatusMessage string `json:"response_status_message,omitempty"`
}

type Task_HttpRequest struct {
	// Required. The full url path that the request will be sent to.
	//
	// This string must begin with either "http://" or "https://". Some examples
	// are: `http://acme.com` and `https://acme.com/sales:8080`. Cloud Tasks will
	// encode some characters for safety and compatibility. The maximum allowed
	// URL length is 2083 characters after encoding.
	//
	// The `Location` header response from a redirect response [`300` - `399`]
	// may be followed. The redirect is not counted as a separate attempt.
	Url string `json:"url,omitempty"`
	// The HTTP method to use for the request. The default is POST.
	HttpMethod cloudtaskspb.HttpMethod `json:"http_method,omitempty"`
	// HTTP request headers.
	//
	// This map contains the header field names and values.
	// Headers can be set when the
	// [task is created][google.cloud.tasks.v2beta3.CloudTasks.CreateTask].
	//
	// These headers represent a subset of the headers that will accompany the
	// task's HTTP request. Some HTTP request headers will be ignored or replaced.
	//
	// A partial list of headers that will be ignored or replaced is:
	//
	//   - Host: This will be computed by Cloud Tasks and derived from
	//     [HttpRequest.url][google.cloud.tasks.v2.HttpRequest.url].
	//   - Content-Length: This will be computed by Cloud Tasks.
	//   - User-Agent: This will be set to `"Google-Cloud-Tasks"`.
	//   - `X-Google-*`: Google use only.
	//   - `X-AppEngine-*`: Google use only.
	//
	// `Content-Type` won't be set by Cloud Tasks. You can explicitly set
	// `Content-Type` to a media type when the
	//
	//	[task is created][google.cloud.tasks.v2beta3.CloudTasks.CreateTask].
	//	For example, `Content-Type` can be set to `"application/octet-stream"` or
	//	`"application/json"`.
	//
	// Headers which can have multiple values (according to RFC2616) can be
	// specified using comma-separated values.
	//
	// The size of the headers must be less than 80KB.
	Headers map[string]string `json:"headers,omitempty"`
	// HTTP request body.
	//
	// A request body is allowed only if the
	// [HTTP method][google.cloud.tasks.v2.HttpRequest.http_method] is POST, PUT,
	// or PATCH. It is an error to set body on a task with an incompatible
	// [HttpMethod][google.cloud.tasks.v2.HttpMethod].
	Body []byte `json:"body,omitempty"`
}

// ToCloudTasksTask converts a db.Task to a cloudtaskspb.Task.
func (t *Task) ToCloudTasksTask(view cloudtaskspb.Task_View) *cloudtaskspb.Task {
	var scheduleTime *timestamppb.Timestamp
	if t.ScheduleTime != nil {
		scheduleTime = timestamppb.New(*t.ScheduleTime)
	}
	var createTime *timestamppb.Timestamp
	if t.CreateTime != nil {
		createTime = timestamppb.New(*t.CreateTime)
	}
	var dispatchDeadline *durationpb.Duration
	if t.DispatchDeadline != nil {
		dispatchDeadline = durationpb.New(*t.DispatchDeadline)
	}
	var httpRequest *cloudtaskspb.HttpRequest
	if t.HttpRequest != nil {
		httpRequest = &cloudtaskspb.HttpRequest{
			Url:        t.HttpRequest.Url,
			HttpMethod: t.HttpRequest.HttpMethod,
			Headers:    t.HttpRequest.Headers,
			Body:       t.HttpRequest.Body,
		}
	}

	var firstAttempt *cloudtaskspb.Attempt
	if t.FirstAttempt != nil {
		firstAttempt = &cloudtaskspb.Attempt{}
		if t.FirstAttempt.ScheduleTime != nil {
			firstAttempt.ScheduleTime = timestamppb.New(*t.FirstAttempt.ScheduleTime)
		}
		if t.FirstAttempt.DispatchTime != nil {
			firstAttempt.DispatchTime = timestamppb.New(*t.FirstAttempt.DispatchTime)
		}
		if t.FirstAttempt.ResponseTime != nil {
			firstAttempt.ResponseTime = timestamppb.New(*t.FirstAttempt.ResponseTime)
		}
		// Reconstruct ResponseStatus from stored code/message if present.
		if t.FirstAttempt.ResponseStatusCode != 0 || t.FirstAttempt.ResponseStatusMessage != "" {
			firstAttempt.ResponseStatus = &status.Status{
				Code:    t.FirstAttempt.ResponseStatusCode,
				Message: t.FirstAttempt.ResponseStatusMessage,
			}
		}
	}

	var lastAttempt *cloudtaskspb.Attempt
	if t.LastAttempt != nil {
		lastAttempt = &cloudtaskspb.Attempt{}
		if t.LastAttempt.ScheduleTime != nil {
			lastAttempt.ScheduleTime = timestamppb.New(*t.LastAttempt.ScheduleTime)
		}
		if t.LastAttempt.DispatchTime != nil {
			lastAttempt.DispatchTime = timestamppb.New(*t.LastAttempt.DispatchTime)
		}
		if t.LastAttempt.ResponseTime != nil {
			lastAttempt.ResponseTime = timestamppb.New(*t.LastAttempt.ResponseTime)
		}
		// Reconstruct ResponseStatus from stored code/message if present.
		if t.LastAttempt.ResponseStatusCode != 0 || t.LastAttempt.ResponseStatusMessage != "" {
			lastAttempt.ResponseStatus = &status.Status{
				Code:    t.LastAttempt.ResponseStatusCode,
				Message: t.LastAttempt.ResponseStatusMessage,
			}
		}
	}

	task := &cloudtaskspb.Task{
		Name:             t.Name,
		ScheduleTime:     scheduleTime,
		CreateTime:       createTime,
		DispatchDeadline: dispatchDeadline,
		DispatchCount:    t.DispatchCount,
		ResponseCount:    t.ResponseCount,
		FirstAttempt:     firstAttempt,
		LastAttempt:      lastAttempt,
		// this is not implemented, we return what the caller sends.
		// ideally we would only return data for that specific view.
		View: view,
	}

	// Set message type based on what we have
	if httpRequest != nil {
		task.MessageType = &cloudtaskspb.Task_HttpRequest{
			HttpRequest: httpRequest,
		}
	}

	return task
}
