module github.com/tools/pd-api-bench

go 1.20

require (
	github.com/tikv/pd v1.1.0-beta.0.20230803041908-c8c5e1e215a1
	github.com/tikv/pd/client v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.24.0
	google.golang.org/grpc v1.54.0
)

require (
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/gorilla/mux v1.7.4 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/errcode v0.3.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c // indirect
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pingcap/kvproto v0.0.0-20230727073445-53e1f8730c30 // indirect
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3 // indirect
	github.com/prometheus/client_golang v1.11.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/unrolled/render v1.0.1 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/tikv/pd/client => ../../client

replace github.com/pingcap/kvproto => github.com/HuSharp/kvproto v0.0.0-20230803025623-dfd11f435356
