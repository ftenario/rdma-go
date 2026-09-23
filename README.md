# Go RDMA file transfer

This directory contains a Go implementation of the same two-part RDMA file transfer workflow used by the C programs in the parent folder, but with Go naming so it does not conflict with the original C binaries.

The Go executables are named with a `_go` suffix to avoid collisions with the existing Linux C tools:

- `rdma_file_send_go`
- `rdma_file_recv_go`

This code is intentionally kept separate from the C version and is intended to be compiled on a Linux RDMA host with `libibverbs` and the Mellanox drivers installed.

## Important notes

- This implementation uses cgo and requires a Linux environment with RDMA headers and `libibverbs` available.
- It is designed to match the same file-transfer flow used by the C version.
- The binary names intentionally avoid conflict with the C counterparts in the parent folder.

## Folder layout

```text
rdma-go/
├── Makefile
├── README.md
├── go.mod
├── send/
│   └── main.go
├── recv/
│   └── main.go
├── rdma_file_send_go
└── rdma_file_recv_go
```

## Build

From this directory:

```bash
make
```

This builds the Go variants:

```bash
./rdma_file_send_go --help
./rdma_file_recv_go --help
```

## Run the receiver

```bash
./rdma_file_recv_go --summary /tmp/received.bin
```

## Run the sender

```bash
./rdma_file_send_go --summary 10.0.0.2 /tmp/sample_4G.bin
```

## Supported flags

- `--verbose` / `-v`
- `--summary` / `-s`
- `--quiet` / `-q`

## Linux requirement

This project depends on `libibverbs` and the Mellanox RDMA stack. Build and run it only on Linux hosts or Linux VMs with the Mellanox drivers installed.

For the underlying RDMA concepts and Linux setup flow, also refer to the parent project guide:

- [../RDMA_GUIDE.md](../RDMA_GUIDE.md)
