include ./.env
export

run:
	go run main.go

test:
	go test -v ./...

build:
	go build -o pg-mvcc cmd/root.go

clean:
	rm -f pg-mvcc
