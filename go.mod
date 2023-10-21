module github.com/gly-hub/grpc-etcd

go 1.20

require (
	github.com/gly-hub/libkv v0.0.0
	go.etcd.io/etcd/client/v2 v2.305.9
)

replace github.com/gly-hub/libkv v0.0.0 => ../libkv

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
)
