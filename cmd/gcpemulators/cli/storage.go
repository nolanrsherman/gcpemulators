package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/nolanrsherman/gcpemulators/cloudstorageemulator"
	"github.com/nolanrsherman/gcpemulators/cloudstorageemulator/db"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	storagePort     int
	storageMongoURI string
	storageDBName   string
)

func newStorageCmd(ctx context.Context, logger *zap.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Start the Cloud Storage emulator",
		Long: `Start the Cloud Storage emulator that provides a local implementation
of the Google Cloud Storage API for testing purposes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStorage(ctx, logger)
		},
	}

	cmd.Flags().IntVarP(&storagePort, "port", "p", 6351, "gRPC server port")
	cmd.Flags().StringVar(&storageMongoURI, "mongo-uri", "mongodb://localhost:27017/?directConnection=true", "MongoDB connection URI")
	cmd.Flags().StringVar(&storageDBName, "db-name", "cloudstorageemulator", "MongoDB database name")

	return cmd
}

func runStorage(ctx context.Context, logger *zap.Logger) error {
	logger.Info("starting Cloud Storage emulator",
		zap.Int("port", storagePort),
		zap.String("mongo_uri", storageMongoURI),
		zap.String("db_name", storageDBName),
	)

	// Run migrations
	logger.Info("running database migrations")
	if err := db.RunMigrations(storageMongoURI, storageDBName); err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("failed to run migrations: %w", err)
		}
		logger.Info("no new migrations to run")
	}

	// Connect to MongoDB
	logger.Info("connecting to MongoDB")
	mongoClient, err := db.NewConnection(ctx, storageMongoURI)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(ctx); err != nil {
			logger.Error("failed to disconnect from MongoDB", zap.Error(err))
		}
	}()

	mongoDB := mongoClient.Database(storageDBName)

	// Create and start emulator
	emulator := cloudstorageemulator.NewCloudStorageEmulator(storagePort, logger, mongoDB)

	logger.Info("Cloud Storage emulator started",
		zap.Int("port", storagePort),
		zap.String("address", fmt.Sprintf("localhost:%d", storagePort)),
	)

	if err := emulator.Start(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Cloud Storage emulator stopped gracefully")
			return nil
		}
		return fmt.Errorf("emulator error: %w", err)
	}

	return nil
}
