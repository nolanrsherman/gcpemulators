package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	cloudTasksPort     int
	cloudTasksMongoURI string
	cloudTasksDBName   string
)

func newCloudTasksCmd(ctx context.Context, logger *zap.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cloudtasks",
		Short: "Start the Cloud Tasks emulator",
		Long: `Start the Cloud Tasks emulator that provides a local implementation
of the Google Cloud Tasks API for testing purposes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCloudTasks(ctx, logger)
		},
	}

	cmd.Flags().IntVarP(&cloudTasksPort, "port", "p", 6352, "gRPC server port")
	cmd.Flags().StringVar(&cloudTasksMongoURI, "mongo-uri", "mongodb://localhost:27017/?directConnection=true", "MongoDB connection URI")
	cmd.Flags().StringVar(&cloudTasksDBName, "db-name", "cloudtaskemulator", "MongoDB database name")

	return cmd
}

func runCloudTasks(ctx context.Context, logger *zap.Logger) error {
	logger.Info("starting Cloud Tasks emulator",
		zap.Int("port", cloudTasksPort),
		zap.String("mongo_uri", cloudTasksMongoURI),
		zap.String("db_name", cloudTasksDBName),
	)

	// Create and run emulator
	emulator := cloudtaskemulator.NewCloudTaskEmulator(cloudTasksMongoURI, cloudTasksDBName, logger, cloudTasksPort)

	logger.Info("Cloud Tasks emulator started",
		zap.Int("port", cloudTasksPort),
		zap.String("address", fmt.Sprintf("localhost:%d", cloudTasksPort)),
	)

	if err := emulator.Run(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Cloud Tasks emulator stopped gracefully")
			return nil
		}
		return fmt.Errorf("emulator error: %w", err)
	}

	return nil
}
