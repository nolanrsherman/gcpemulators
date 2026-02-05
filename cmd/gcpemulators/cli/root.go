package cli

import (
	"context"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   "gcpemulators",
	Short: "GCP Emulators - A collection of GCP emulators for testing",
	Long: `GCP Emulators provides emulators for various Google Cloud Platform services
designed to be native to Go and easy to integrate into your test environment.`,
}

// Execute runs the root command
func Execute(ctx context.Context, logger *zap.Logger) error {
	rootCmd.AddCommand(newStorageCmd(ctx, logger))
	rootCmd.AddCommand(newCloudTasksCmd(ctx, logger))
	rootCmd.AddCommand(newVersionCmd())
	return rootCmd.ExecuteContext(ctx)
}
