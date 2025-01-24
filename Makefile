DIRS := $(shell go work edit -json | grep 'DiskPath' | awk -F '"' '{print $$4}')
STARTING := $(shell pwd)

echo:
	echo $(DIRS)

dirs:
	@for f in $(DIRS); do echo $$f; done;

deps:
	@for f in $(DIRS); do echo "running on $$f" && cd $$f && go get -v -t ./...; cd $(STARTING); done;

update-golly:
	@for f in $(DIRS); do \
		echo $$f && \
		cd $$f && \
		GOPROXY=direct go get github.com/golly-go/golly@main && \
		go mod tidy && \
		cd $(STARTING); \
	done

update-urls:
	@for f in $(DIRS); do echo go get github.com/golly-go/plugins/$$f; done;

vet:
	@for f in $(DIRS); do echo "running on $$f" && cd $$f && go vet ./...; cd $(STARTING); done;

tests:
	@for f in $(DIRS); do echo "running on $$f" && cd $$f && go test ./... -cover; cd $(STARTING); done;

tidy:
	@for f in $(DIRS); do \
	  echo "Running go mod tidy in $$f..."; \
	  cd $$f && go mod tidy; \
	  cd $(STARTING); \
	  echo "Finished $$f"; \
	done;

update:
	@for f in $(DIRS); do \
	  echo "Running go get -u in $$f..."; \
	  cd $$f && go get -u ./...; \
	  cd $(STARTING); \
	  echo "Updated $$f"; \
	done;

clean-cache:
	@echo "Cleaning Go module cache..."
	go clean -modcache
	@echo "Cache cleaned!"

graph:
	@for f in $(DIRS); do \
	  echo "Generating dependency graph for $$f..."; \
	  cd $$f && go mod graph > $$f-dep-graph.txt; \
	  cd $(STARTING); \
	  echo "Graph generated for $$f"; \
	done;

help:
	@echo "Available commands:"
	@echo "  tidy          - Run 'go mod tidy' in each directory"
	@echo "  update        - Update dependencies in each directory"
	@echo "  clean-cache   - Clear Go module cache"
	@echo "  graph         - Generate dependency graph for each directory"