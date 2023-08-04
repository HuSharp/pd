pd-api-bench
========

pd-api-bench is a tool to test PD API.

## Build
1. [Go](https://golang.org/) Version 1.19 or later
2. In the root directory of the [PD project](https://github.com/tikv/pd), use the `make` command to compile and generate `bin/pd-api-bench`


## Usage

This section describes how to use the `pd-api-bench` tool.

### Flags description

```
-cacert string
  path of file that contains list of trusted SSL CAs
-cert string
  path of file that contains X509 certificate in PEM format
-concurrency int
  the client number (default 1)
-pd string
  pd address (default "127.0.0.1:2379")
-qps 
  the qps of request (default 1000)
```

### TLS

You can use the following command to generate a certificate for testing TLS:

```shell
cd cert
./generate_cert.sh
go run ../main.go --min-resolved-ts-http -cacert ca.pem -cert pd-server.pem  -key pd-server-key.pem 

```