.PHONY: install build test lint vet fmt clean

install: fmt test
	go install -v

test:
	go test -race -p 1 ./...

lint:
	golint ./...

vet:
	go vet ./...

fmt:
	! gofmt -d -e -s *.go **/*.go 2>&1 | tee /dev/tty | read

clean:
	go clean
