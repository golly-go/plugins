DIRS = $(shell ls -d */)
STARTING =  $(shell pwd)

dirs:
	@for f in $(DIRS); do echo $$f; done;

deps:
	@for f in $(DIRS); do echo running on $$f && cd $$f && go get -v -t -d ./...; cd ${STARTING}; done;

update-golly:
	@for f in $(DIRS); do cd $$f && GOPROXY=direct go get -v -u github.com/golly-go/golly; cd ${STARTING}; done;

update-urls:
	@for f in $(DIRS); do echo go get github.com/golly-go/plugins/$$f; done;

vet:
	@for f in $(DIRS); do echo running on $$f && cd $$f && go vet ./...; cd ${STARTING}; done;

tests:
	@for f in $(DIRS); do echo running on $$f && cd $$f && go test ./... -cover; cd ${STARTING}; done;
