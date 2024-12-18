# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=kubsto
DB_FILE=./data

# All target
all: test build

# Build the project
build:
	$(GOBUILD) -o bin/$(BINARY_NAME) -v

# Run tests
test: 
	$(GOTEST) -v ./...

# Clean the build files
clean: 
	$(GOCLEAN)
	rm -f bin/$(BINARY_NAME)

# Clean database
clean-db: 
	rm -r $(DB_FILE)

# Install dependencies
deps: 
	$(GOGET) -v ./...

# Go mod tidy
mod-tidy: 
	$(GOCMD) mod tidy

.PHONY: all build clean test deps build-linux run mod-tidy