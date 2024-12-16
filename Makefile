# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=kubsto

# All target
all: test build

# Build the project
build:
	$(GOBUILD) -o $(BINARY_NAME) -v

# Run tests
test: 
	$(GOTEST) -v ./...

# Clean the build files
clean: 
	$(GOCLEAN)
	rm -f bin/$(BINARY_NAME)

# Install dependencies
deps: 
	$(GOGET) -v ./...

# Go mod tidy
mod-tidy: 
	$(GOCMD) mod tidy

# Cross compile for Linux
build-linux: 
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o bin/$(BINARY_NAME) -v

.PHONY: all build clean test deps build-linux run mod-tidy