package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const testMongoURI = "mongodb://localhost:27017/?directConnection=true"

func TestRunMigrations(t *testing.T) {
	ctx := context.Background()
	db, cleanup := NewTestDatabase(ctx, t, testMongoURI)
	defer cleanup(t)
	err := RunMigrations(testMongoURI, db.Name())
	require.NoError(t, err)
}
