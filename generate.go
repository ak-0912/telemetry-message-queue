// Package telemetrymessagequeue exists only to host protobuf code generation hooks.
//
// The official protoc binary is not installable via Go modules. Use Buf (install with
// go install github.com/bufbuild/buf/cmd/buf@latest) to parse .proto files and invoke
// the Go plugins remotely—no separate protoc install required.
//
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.42.0 generate
package telemetrymessagequeue
