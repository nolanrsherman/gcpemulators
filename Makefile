.PHONY: build-version minor-version docker-build docker-push docker-build-push buf help publish-tag publish-minor-version

# Docker image name
IMAGE_NAME := nolanrs/gcpemulators

# Read current version from version.txt
VERSION := $(shell cat version.txt | tr -d '\n' | tr -d ' ')

# Extract version components (assumes format vMAJOR.MINOR.PATCH)
VERSION_PARTS := $(subst ., ,$(subst v,,$(VERSION)))
MAJOR := $(word 1,$(VERSION_PARTS))
MINOR := $(word 2,$(VERSION_PARTS))
PATCH := $(word 3,$(VERSION_PARTS))

help:
	@echo "Available targets:"
	@echo "  buf              - Generate protobuf client and server code using buf"
	@echo "  build-version    - Increment build/patch version (e.g., v0.1.2 -> v0.1.3)"
	@echo "  minor-version    - Increment minor version (e.g., v0.1.2 -> v0.2.0)"
	@echo "  docker-build     - Build Docker image with current version and latest tags"
	@echo "  docker-push      - Push Docker image (both version and latest tags)"
	@echo "  docker-build-push - Build and push Docker image"
	@echo "  publish-tag      - Create and push a git tag based on current version"
	@echo "  publish-minor-version - Create and push a git tag based on current minor version"
	@echo ""
	@echo "Current version: $(VERSION)"

# Generate protobuf client and server code using buf
buf:
	@echo "Generating protobuf code with buf..."
	@buf generate
	@echo "Protobuf code generation complete!"

# Increment build/patch version (e.g., v0.1.2 -> v0.1.3)
build-version:
	@echo "Current version: $(VERSION)"
	@NEW_PATCH=$$(($(PATCH) + 1)); \
	NEW_VERSION="v$(MAJOR).$(MINOR).$$NEW_PATCH"; \
	echo "New version: $$NEW_VERSION"; \
	echo "$$NEW_VERSION" > version.txt; \
	echo "Updated version.txt to $$NEW_VERSION"

# Increment minor version (e.g., v0.1.2 -> v0.2.0)
minor-version:
	@echo "Current version: $(VERSION)"
	@NEW_MINOR=$$(($(MINOR) + 1)); \
	NEW_VERSION="v$(MAJOR).$$NEW_MINOR.0"; \
	echo "New version: $$NEW_VERSION"; \
	echo "$$NEW_VERSION" > version.txt; \
	echo "Updated version.txt to $$NEW_VERSION"
	git add version.txt
	git commit -m "Increment minor version: $(VERSION) => $$NEW_VERSION"

# Build Docker image with current version and latest tags
docker-build:
	@echo "Building Docker image..."
	@echo "Current version: $(VERSION)"
	@docker build -t $(IMAGE_NAME):$(VERSION) .
	@docker tag $(IMAGE_NAME):$(VERSION) $(IMAGE_NAME):latest
	@echo "Built and tagged:"
	@echo "  - $(IMAGE_NAME):$(VERSION)"
	@echo "  - $(IMAGE_NAME):latest"

# Push Docker image (both version and latest tags)
docker-push:
	@echo "Pushing Docker image..."
	@echo "Current version: $(VERSION)"
	@docker push $(IMAGE_NAME):$(VERSION)
	@docker push $(IMAGE_NAME):latest
	@echo "Pushed:"
	@echo "  - $(IMAGE_NAME):$(VERSION)"
	@echo "  - $(IMAGE_NAME):latest"

# Build and push Docker image
docker-build-push: docker-build docker-push
	@echo "Build and push complete!"

# Create and push a git tag based on current version
publish-tag:
	@echo "Publishing git tag for version: $(VERSION)"
	@if git rev-parse "$(VERSION)" >/dev/null 2>&1; then \
		echo "Error: Tag $(VERSION) already exists"; \
		exit 1; \
	fi
	@git tag -a "$(VERSION)" -m "Release $(VERSION)"
	@echo "Created tag: $(VERSION)"
	@echo "Pushing tag to remote..."
	@git push origin "$(VERSION)"
	@echo "Tag $(VERSION) published successfully!"
