package cloudtaskemulator

import (
	"context"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Gets the access control policy for a [Queue][google.cloud.tasks.v2.Queue].
// Returns an empty policy if the resource exists and does not have a policy
// set.
//
// Authorization requires the following
// [Google IAM](https://cloud.google.com/iam) permission on the specified
// resource parent:
//
// * `cloudtasks.queues.getIamPolicy`
func (s *CloudTaskEmulator) GetIamPolicy(context.Context, *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetIamPolicy not implemented")
}

// Sets the access control policy for a [Queue][google.cloud.tasks.v2.Queue].
// Replaces any existing policy.
//
// Note: The Cloud Console does not check queue-level IAM permissions yet.
// Project-level permissions are required to use the Cloud Console.
//
// Authorization requires the following
// [Google IAM](https://cloud.google.com/iam) permission on the specified
// resource parent:
//
// * `cloudtasks.queues.setIamPolicy`
func (s *CloudTaskEmulator) SetIamPolicy(context.Context, *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetIamPolicy not implemented")
}

// Returns permissions that a caller has on a
// [Queue][google.cloud.tasks.v2.Queue]. If the resource does not exist, this
// will return an empty set of permissions, not a
// [NOT_FOUND][google.rpc.Code.NOT_FOUND] error.
//
// Note: This operation is designed to be used for building permission-aware
// UIs and command-line tools, not for authorization checking. This operation
// may "fail open" without warning.
func (s *CloudTaskEmulator) TestIamPermissions(context.Context, *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TestIamPermissions not implemented")
}
