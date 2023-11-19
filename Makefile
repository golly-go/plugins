DIRS = $(shell ls -d */)
PWD =  $(shell pwd)

dirs:
	@for f in $(DIRS); do echo $$f; done;

deps:
	@for f in $(DIRS); do cd $$f && go get -v -t -d ./... && cd $(PWD); done;

update-golly:
	@for f in $(DIRS); do cd $$f && GOPROXY=direct go get -v -u github.com/golly-go/golly && cd $(PWD); done;

update-urls:
	@for f in $(DIRS); do echo go get github.com/golly-go/plugins/$$f;  done;

vet:
	@for f in $(DIRS); do pushd $$f && go vet ./... && cd $(PWD); done;

tests:
	@for f in $(DIRS); do pushd $$f && go test ./... -cover && cd $(PWD); done;
