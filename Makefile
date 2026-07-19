# KernelQ root Makefile — code generation helpers.

.PHONY: proto
proto:
	@mkdir -p worker/internal/grpc/pb
	PATH="$(shell go env GOPATH)/bin:$$PATH" protoc \
		--proto_path=proto \
		--go_out=worker/internal/grpc/pb --go_opt=paths=source_relative \
		--go-grpc_out=worker/internal/grpc/pb --go-grpc_opt=paths=source_relative \
		proto/worker_execution.proto
	@echo "generated worker/internal/grpc/pb from proto/worker_execution.proto"
