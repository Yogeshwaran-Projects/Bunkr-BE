PROTO_DIR = proto

.PHONY: build test proto clean run

build:
	@go build -o bin/bunkrd ./cmd/bunkrd
	@go build -o bin/bunkr ./cmd/bunkr
	@echo "Built: bin/bunkrd, bin/bunkr"

test:
	@go test ./... -v -race -count=1

run: build
	@./bin/bunkrd

proto:
	@PATH="$$PATH:$$(go env GOPATH)/bin" protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raft.proto

clean:
	@rm -rf bin/ bunkr-data/
