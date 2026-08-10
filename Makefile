.PHONY: proto fmt test test-race build run run-worker infra-up infra-down

# 生成 protobuf 与 gRPC 代码。生成结果提交到仓库，部署机无需安装 protoc。
proto:
	protoc -I api --go_out=. --go_opt=module=github.com/licy-yu/agent-os \
		--go-grpc_out=. --go-grpc_opt=module=github.com/licy-yu/agent-os \
		api/controlplane/v1/controlplane.proto

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './web/node_modules/*')

test:
	go test ./...

test-race:
	go test -race ./...

build:
	go build -trimpath -o bin/control-plane ./cmd/control-plane
	go build -trimpath -o bin/worker ./cmd/worker

run:
	go run ./cmd/control-plane -config ./configs/config.yaml

run-worker:
	go run ./cmd/worker -config ./configs/config.yaml

infra-up:
	docker compose up -d --wait postgres redis nats

infra-down:
	docker compose down
