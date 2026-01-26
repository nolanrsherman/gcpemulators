// This package is an emualator for GCP Cloud Tasks.
// It can be used to test cloud tasks locally and supports
// the most common features of Cloud Tasks.
// 1. Cloud task API - Managing Queues and Tasks.
// 2. Delivery to task targets with HTTP, with all Retry logic, timeouts.
// 3. Implementation of features like Task Deduplication, and Rate Limiting.
package cloudtaskemulator

import (
	"context"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server implements the Cloud Tasks gRPC service interface.
// It provides an emulator for GCP Cloud Tasks that can be used for local testing.
type Server struct {
	cloudtaskspb.UnimplementedCloudTasksServer
}

func (s *Server) ListQueues(context.Context, *cloudtaskspb.ListQueuesRequest) (*cloudtaskspb.ListQueuesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListQueues not implemented")
}
func (s *Server) GetQueue(context.Context, *cloudtaskspb.GetQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetQueue not implemented")
}
func (s *Server) CreateQueue(context.Context, *cloudtaskspb.CreateQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateQueue not implemented")
}
func (s *Server) UpdateQueue(context.Context, *cloudtaskspb.UpdateQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateQueue not implemented")
}
func (s *Server) DeleteQueue(context.Context, *cloudtaskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteQueue not implemented")
}
func (s *Server) PurgeQueue(context.Context, *cloudtaskspb.PurgeQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PurgeQueue not implemented")
}
func (s *Server) PauseQueue(context.Context, *cloudtaskspb.PauseQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PauseQueue not implemented")
}
func (s *Server) ResumeQueue(context.Context, *cloudtaskspb.ResumeQueueRequest) (*cloudtaskspb.Queue, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ResumeQueue not implemented")
}
func (s *Server) GetIamPolicy(context.Context, *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetIamPolicy not implemented")
}
func (s *Server) SetIamPolicy(context.Context, *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetIamPolicy not implemented")
}
func (s *Server) TestIamPermissions(context.Context, *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TestIamPermissions not implemented")
}
func (s *Server) ListTasks(context.Context, *cloudtaskspb.ListTasksRequest) (*cloudtaskspb.ListTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListTasks not implemented")
}
func (s *Server) GetTask(context.Context, *cloudtaskspb.GetTaskRequest) (*cloudtaskspb.Task, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTask not implemented")
}
func (s *Server) CreateTask(context.Context, *cloudtaskspb.CreateTaskRequest) (*cloudtaskspb.Task, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTask not implemented")
}
func (s *Server) DeleteTask(context.Context, *cloudtaskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteTask not implemented")
}
func (s *Server) RunTask(context.Context, *cloudtaskspb.RunTaskRequest) (*cloudtaskspb.Task, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunTask not implemented")
}
