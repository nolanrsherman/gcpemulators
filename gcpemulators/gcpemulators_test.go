package gcpemulators

import (
	"context"
	"testing"

	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/cloudtasksemulatorpb"
	"github.com/stretchr/testify/require"
)

func TestNewCloudTaskEmulator(t *testing.T) {
	cloudTasksClient, cleanup, err := NewCloudTaskEmulator()
	require.NoError(t, err)
	defer func(t *testing.T) {
		t.Helper()
		err := cleanup()
		require.NoError(t, err)
	}(t)
	require.NotNil(t, cloudTasksClient)
	require.NotNil(t, cleanup)

	readiness, err := cloudTasksClient.Readiness(context.Background(), &cloudtasksemulatorpb.ReadinessRequest{})
	require.NoError(t, err)
	require.True(t, readiness.Ready)

}
