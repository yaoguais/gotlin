#!/bin/sh

protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    --go_opt=Mgotlin.proto=github.com/yaoguais/gotlin \
    gotlin.proto

