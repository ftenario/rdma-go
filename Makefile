GO ?= go
UNAME := $(shell uname)

.PHONY: all clean

all: rdma_file_send_go rdma_file_recv_go

ifeq ($(UNAME), Darwin)
all:
	$(error libibverbs is Linux-only. Copy the Go project to a Linux RDMA host and run make there)
endif

rdma_file_send_go: send/main.go
	$(GO) build -o $@ ./send

rdma_file_recv_go: recv/main.go
	$(GO) build -o $@ ./recv

clean:
	rm -f rdma_file_send_go rdma_file_recv_go
