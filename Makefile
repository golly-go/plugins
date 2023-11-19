DIRS = $(shell ls -d */)


dirs:
	@for f in $(DIRS); do echo $$f; done;

deps:
	@for f in $(DIRS); do pushd $$f && go get -v -t -d ./... && popd; done;

update-golly:
	@for f in $(DIRS); do pushd $$f && GOPROXY=direct go get -v -u github.com/golly-go/golly && popd; done;

update-urls:
	@for f in $(DIRS); do echo go get github.com/golly-go/plugins/$$f;  done;


vet:
	@for f in $(DIRS); do pushd $$f && go vet ./... && popd; done;

tests:
	@for f in $(DIRS); do pushd $$f && go test ./... -cover && popd; done;
