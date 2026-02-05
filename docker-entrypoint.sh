#!/bin/sh
set -e

# Start MongoDB in the background
echo "Starting MongoDB..."
# Create log directory if it doesn't exist
mkdir -p /var/log/mongodb
# Start MongoDB as a background process
mongod --fork --logpath /var/log/mongodb/mongod.log --bind_ip_all

# Wait for MongoDB to be ready (health check instead of fixed sleep)
echo "Waiting for MongoDB to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
        echo "MongoDB is ready"
        break
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done
if [ $attempt -eq $max_attempts ]; then
    echo "Warning: MongoDB may not be fully ready"
fi

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
/usr/local/bin/gcpemulators "$@" &
PID=$!

# Wait for the Go process to exit
wait "$PID"
EXIT_CODE=$?

# Cleanup on normal exit (only shutdown MongoDB, don't exit)
echo "Shutting down MongoDB..."
mongod --shutdown 2>/dev/null || true

exit $EXIT_CODE
