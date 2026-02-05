# GCP Emulators
# Description: A Docker image containing GCP service emulators
# with embedded MongoDB for local testing and development. Provides gRPC-compatible 
# emulators that mirror production GCP services for offline development and testing workflows. 

# Build stage
FROM golang:1.25-bookworm AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary - detect architecture and build accordingly
# Build to a different name to avoid conflicts with any gcpemulators directory
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$(go env GOARCH) go build -o /app/gcpemulators-bin ./cmd/gcpemulators

# Runtime stage
FROM debian:bookworm-slim

# Install MongoDB from official repository
# Install ca-certificates first (required for HTTPS with curl)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates gnupg curl && \
    curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor && \
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list && \
    apt-get update && \
    apt-get install -y mongodb-org mongodb-mongosh file && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create directories for MongoDB data and logs
RUN mkdir -p /data/db /var/log/mongodb

WORKDIR /app
COPY --from=builder /app/gcpemulators-bin /usr/local/bin/gcpemulators
# Include migration files
COPY ./cloudtaskemulator/db/migrations ./cloudtaskemulator/db/migrations
COPY ./cloudstorageemulator/db/migrations ./cloudstorageemulator/db/migrations

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose default ports for emulators
EXPOSE 6351 6352 27017

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Default command (shows help if no command provided)
CMD ["--help"]
