package db

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewConnection(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().
		ApplyURI(uri).
		SetBSONOptions(&options.BSONOptions{
			UseJSONStructTags: true,
		})
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// RunMigrations runs database migrations on the test database using golang-migrate.
// This function should be called in your test setup after creating a test database with NewTestDatabase.
//
// By default, it runs all pending migrations up to the latest version. Migrations are
// loaded from the mongomigrations directory relative to this source file's location.
//
// Usage:
//
//	// Run all migrations
//	db, cleanup := mongotest.NewTestDatabase(ctx, t)
//	defer cleanup(t)
//	mongotest.RunMigrations(t, db)
//
//	// Run migrations up to a specific version
//	mongotest.RunMigrations(t, db, mongotest.WithVersion(5))
//
//	// Customize source URI (where migration files are located)
//	mongotest.RunMigrations(t, db, mongotest.WithSourceURI("file://./custom/path"))
//
// Options can be combined:
//
//	mongotest.RunMigrations(t, db,
//		mongotest.WithVersion(3),
//		mongotest.WithSourceURI("file://./migrations"),
//	)
func RunMigrations(connURI string, dbName string) error {

	// Get the path relative to this source file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("failed to get caller information")
	}
	// This file is in internal/db/mongotest/, migrations are in internal/db/mongomigrations/
	migrationsDir := filepath.Join(filepath.Dir(filename), "./migrations")
	migrationsPath, err := filepath.Abs(migrationsDir)
	if err != nil {
		return fmt.Errorf("failed to resolve migrations path: %w", err)
	}
	sourceURI := fmt.Sprintf("file://%s", migrationsPath)

	u, err := url.Parse(connURI)
	if err != nil {
		return fmt.Errorf("failed to parse connection URI: %w", err)
	}
	u.Path = dbName
	connectionURI := u.String()

	// Run the migrations using golang-migrate
	m, err := migrate.New(sourceURI, connectionURI)
	if err != nil {
		return fmt.Errorf("failed to create migrate: %w", err)
	}
	err = m.Up()
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	return nil
}

func NewTestDatabase(ctx context.Context, t *testing.T, uri string) (*mongo.Database, func(t *testing.T)) {
	t.Helper()
	// Create a new mongo connection
	client, err := NewConnection(ctx, uri)
	if err != nil {
		t.Fatalf("failed to create mongo connection: %v", err)
	}

	dbName := fmt.Sprintf("test_cloudtaskemulator_%s", t.Name())
	dbName = strings.ReplaceAll(dbName, "/", "_")
	if len(dbName) > 60 {
		dbName = dbName[:60]
	}

	// Create a database for the test
	db := client.Database(dbName)

	// Return the connection and a cleanup function
	return db, func(t *testing.T) {
		t.Helper()
		// Drop the database
		err := db.Drop(ctx)
		if err != nil {
			t.Errorf("failed to drop database: %v", err)
		}
		err = client.Disconnect(ctx)
		if err != nil {
			t.Errorf("failed to disconnect from mongo: %v", err)
		}
	}
}
