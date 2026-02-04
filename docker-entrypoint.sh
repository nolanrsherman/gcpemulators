#!/bin/sh
set -e

# Start MongoDB in the background
echo "Starting MongoDB..."
# Create log directory if it doesn't exist
mkdir -p /var/log/mongodb
# Start MongoDB as a background process
mongod --fork --logpath /var/log/mongodb/mongod.log --bind_ip_all

# Wait a moment for MongoDB to start
sleep 2

# Function to handle cleanup and signal forwarding
cleanup() {
    echo "Received signal, shutting down..."
    # Forward signal to Go process
    if [ -n "$PID" ]; then
        kill -TERM "$PID" 2>/dev/null || true
        wait "$PID" 2>/dev/null || true
    fi
    # Shutdown MongoDB
    echo "Shutting down MongoDB..."
    mongod --shutdown 2>/dev/null || true
    exit 0
}

# Set up signal traps
trap cleanup TERM INT

# Start Go process in background and capture PID
go run cmd/gcpemulators/main.go "$@" &
PID=$!

# Wait for the Go process to exit
wait "$PID"
EXIT_CODE=$?

# Cleanup on normal exit (only shutdown MongoDB, don't exit)
echo "Shutting down MongoDB..."
mongod --shutdown 2>/dev/null || true

exit $EXIT_CODE
