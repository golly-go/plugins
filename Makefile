DIRS = $(shell ls -d */)

deps:
	@for f in $(DIRS); do pushd $$f && go get -u ./... && popd; done;

update-golly:
	@for f in $(DIRS); do pushd $$f && GOPROXY=direct go get -u github.com/golly-go/golly && popd; done;