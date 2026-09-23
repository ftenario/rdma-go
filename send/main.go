//go:build linux

package main

/*
#cgo LDFLAGS: -libverbs
#include <infiniband/verbs.h>
#include <stdlib.h>

static struct ibv_context* open_first_device(void) {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if (dev_list == NULL) return NULL;
    struct ibv_context *ctx = NULL;
    if (dev_list[0] != NULL) {
        ctx = ibv_open_device(dev_list[0]);
    }
    ibv_free_device_list(dev_list);
    return ctx;
}

static struct ibv_pd* alloc_pd(struct ibv_context *ctx) {
    return ibv_alloc_pd(ctx);
}

static struct ibv_cq* create_cq(struct ibv_context *ctx) {
    return ibv_create_cq(ctx, 16, NULL, NULL, 0);
}

static struct ibv_qp* create_qp(struct ibv_pd *pd, struct ibv_cq *cq) {
    struct ibv_qp_init_attr attr = {};
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.cap.max_send_wr = 16;
    attr.cap.max_recv_wr = 16;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.qp_type = IBV_QPT_RC;
    return ibv_create_qp(pd, &attr);
}

static struct ibv_mr* reg_mr(struct ibv_pd *pd, void *buf, size_t len) {
    return ibv_reg_mr(pd, buf, len,
        IBV_ACCESS_LOCAL_WRITE |
        IBV_ACCESS_REMOTE_WRITE |
        IBV_ACCESS_REMOTE_READ);
}

static int query_port(struct ibv_context *ctx, uint8_t port_num, struct ibv_port_attr *port_attr) {
    return ibv_query_port(ctx, port_num, port_attr);
}

static int query_gid(struct ibv_context *ctx, uint8_t port_num, int index, union ibv_gid *gid) {
    return ibv_query_gid(ctx, port_num, index, gid);
}

static int modify_qp_init(struct ibv_qp *qp, int port_num) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = (uint8_t)port_num;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    return ibv_modify_qp(qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
}

static int modify_qp_rtr(struct ibv_qp *qp, int dest_qpn, uint16_t dlid, union ibv_gid *gid) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = dest_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.port_num = 1;
    attr.ah_attr.grh.dgid = *gid;
    attr.ah_attr.grh.sgid_index = 3;
    attr.ah_attr.grh.hop_limit = 1;
    return ibv_modify_qp(qp, &attr,
        IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
        IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
}

static int modify_qp_rts(struct ibv_qp *qp) {
    struct ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    return ibv_modify_qp(qp, &attr,
        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
        IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}

static int post_rdma_write(struct ibv_qp *qp, uint64_t src_addr, uint32_t lkey, uint64_t dst_addr, uint32_t rkey, uint32_t length) {
    struct ibv_sge sge = {0};
    sge.addr = src_addr;
    sge.length = length;
    sge.lkey = lkey;

    struct ibv_send_wr wr = {0};
    wr.wr_id = 0;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = dst_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    return ibv_post_send(qp, &wr, &bad);
}

static int wait_send_completion(struct ibv_cq *cq) {
    struct ibv_wc wc = {0};
	int n;
	while ((n = ibv_poll_cq(cq, 1, &wc)) == 0) {
	}
    if (n < 0) return n;
    if (wc.status != IBV_WC_SUCCESS) return (int)wc.status;
    return 0;
}
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
	"unsafe"
)

const (
	port        = 18515
	ibPort      = 1
	gidIndex    = 3
	chunkSize   = 256 * 1024 * 1024
	sendAppName = "rdma_file_send_go"
)

type connInfo struct {
	QPNum      uint32
	LID        uint16
	GID        [16]byte
	RKey       uint32
	RemoteAddr uint64
	FileSize   uint64
}

type chunkMsg struct {
	Offset uint64
	Length uint32
}

func printUsage() {
	fmt.Printf("Usage: %s [--verbose|--summary|--quiet] <server_ip> <file_to_send>\n", sendAppName)
}

func sendAll(conn net.Conn, data []byte) error {
	for written := 0; written < len(data); {
		n, err := conn.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func recvAll(conn net.Conn, data []byte) error {
	for read := 0; read < len(data); {
		n, err := conn.Read(data[read:])
		if err != nil {
			return err
		}
		read += n
	}
	return nil
}

func parseFlags(args []string) (verbose, summary, quiet bool, rest []string, err error) {
	rest = args
	for len(rest) > 0 {
		switch rest[0] {
		case "--verbose", "-v":
			verbose = true
			rest = rest[1:]
		case "--summary", "-s":
			summary = true
			rest = rest[1:]
		case "--quiet", "-q":
			quiet = true
			rest = rest[1:]
		default:
			return verbose, summary, quiet, rest, nil
		}
	}
	return verbose, summary, quiet, rest, nil
}

func fileSize(path string) (uint64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

func setupRDMAContext() (*C.struct_ibv_context, *C.struct_ibv_pd, *C.struct_ibv_cq, *C.struct_ibv_qp, *C.struct_ibv_mr, unsafe.Pointer) {
	ctx := C.open_first_device()
	if ctx == nil {
		return nil, nil, nil, nil, nil, nil
	}

	pd := C.alloc_pd(ctx)
	if pd == nil {
		C.ibv_close_device(ctx)
		return nil, nil, nil, nil, nil, nil
	}

	buf := C.malloc(C.size_t(chunkSize))
	if buf == nil {
		C.ibv_dealloc_pd(pd)
		C.ibv_close_device(ctx)
		return nil, nil, nil, nil, nil, nil
	}

	mr := C.reg_mr(pd, buf, C.size_t(chunkSize))
	if mr == nil {
		C.free(buf)
		C.ibv_dealloc_pd(pd)
		C.ibv_close_device(ctx)
		return nil, nil, nil, nil, nil, nil
	}

	cq := C.create_cq(ctx)
	if cq == nil {
		C.ibv_dereg_mr(mr)
		C.free(buf)
		C.ibv_dealloc_pd(pd)
		C.ibv_close_device(ctx)
		return nil, nil, nil, nil, nil, nil
	}

	qp := C.create_qp(pd, cq)
	if qp == nil {
		C.ibv_destroy_cq(cq)
		C.ibv_dereg_mr(mr)
		C.free(buf)
		C.ibv_dealloc_pd(pd)
		C.ibv_close_device(ctx)
		return nil, nil, nil, nil, nil, nil
	}

	return ctx, pd, cq, qp, mr, buf
}

func activateQP(qp *C.struct_ibv_qp, portNum int) error {
	if C.modify_qp_init(qp, C.int(portNum)) != 0 {
		return errors.New("ibv_modify_qp INIT failed")
	}
	return nil
}

func main() {
	verbose, summary, quiet, rest, err := parseFlags(os.Args[1:])
	if err != nil {
		printUsage()
		os.Exit(1)
	}
	if len(rest) != 2 {
		printUsage()
		os.Exit(1)
	}

	serverIP := rest[0]
	filePath := rest[1]

	fileSizeBytes, err := fileSize(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stat %s: %v\n", filePath, err)
		os.Exit(1)
	}

	fp, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", filePath, err)
		os.Exit(1)
	}
	defer fp.Close()

	ctx, pd, cq, qp, mr, buf := setupRDMAContext()
	if ctx == nil || pd == nil || cq == nil || qp == nil || mr == nil || buf == nil {
		fmt.Fprintln(os.Stderr, "failed to initialize RDMA context")
		os.Exit(1)
	}
	defer func() {
		C.ibv_destroy_qp(qp)
		C.ibv_destroy_cq(cq)
		C.ibv_dereg_mr(mr)
		C.ibv_dealloc_pd(pd)
		C.ibv_close_device(ctx)
		C.free(buf)
	}()

	conn, err := net.Dial("tcp", net.JoinHostPort(serverIP, strconv.Itoa(port)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to %s: %v\n", serverIP, err)
		os.Exit(1)
	}
	defer conn.Close()

	portAttr := C.struct_ibv_port_attr{}
	if C.query_port(ctx, C.uint8_t(ibPort), &portAttr) != 0 {
		fmt.Fprintln(os.Stderr, "query_port failed")
		os.Exit(1)
	}

	gid := C.union_ibv_gid{}
	if C.query_gid(ctx, C.uint8_t(ibPort), C.int(gidIndex), &gid) != 0 {
		fmt.Fprintln(os.Stderr, "query_gid failed")
		os.Exit(1)
	}

	localInfo := connInfo{
		QPNum:      uint32(qp.qp_num),
		LID:        uint16(portAttr.lid),
		RKey:       uint32(mr.rkey),
		RemoteAddr: uint64(uintptr(buf)),
		FileSize:   fileSizeBytes,
	}
	copy(localInfo.GID[:], (*[16]byte)(unsafe.Pointer(&gid))[:])

	localBytes := marshalConnInfo(localInfo)
	if err := sendAll(conn, localBytes); err != nil {
		fmt.Fprintf(os.Stderr, "send local info: %v\n", err)
		os.Exit(1)
	}

	remoteBytes := make([]byte, 42)
	if err := recvAll(conn, remoteBytes); err != nil {
		fmt.Fprintf(os.Stderr, "recv remote info: %v\n", err)
		os.Exit(1)
	}
	remoteInfo := unmarshalConnInfo(remoteBytes)

	if err := activateQP(qp, ibPort); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	remoteGID := C.union_ibv_gid{}
	copy((*[16]byte)(unsafe.Pointer(&remoteGID))[:], remoteInfo.GID[:])
	if C.modify_qp_rtr(qp, C.int(remoteInfo.QPNum), C.uint16_t(remoteInfo.LID), &remoteGID) != 0 {
		fmt.Fprintln(os.Stderr, "ibv_modify_qp RTR failed")
		os.Exit(1)
	}
	if C.modify_qp_rts(qp) != 0 {
		fmt.Fprintln(os.Stderr, "ibv_modify_qp RTS failed")
		os.Exit(1)
	}

	if !quiet {
		fmt.Println("RDMA connection established! Sending file...")
	}

	start := time.Now()
	transferred := uint64(0)
	for transferred < fileSizeBytes {
		remaining := fileSizeBytes - transferred
		n := uint64(chunkSize)
		if remaining < n {
			n = remaining
		}

		chunk := make([]byte, int(n))
		if _, err := fp.Read(chunk); err != nil {
			fmt.Fprintf(os.Stderr, "read file chunk: %v\n", err)
			os.Exit(1)
		}

		bufSlice := (*[chunkSize]byte)(unsafe.Pointer(buf))[:int(n)]
		copy(bufSlice, chunk)

		msg := chunkMsg{Offset: transferred, Length: uint32(n)}
		if err := sendAll(conn, marshalChunkMsg(msg)); err != nil {
			fmt.Fprintf(os.Stderr, "send chunk metadata: %v\n", err)
			os.Exit(1)
		}

		if C.post_rdma_write(qp,
			C.uint64_t(uint64(uintptr(buf))),
			C.uint32_t(mr.lkey),
			C.uint64_t(remoteInfo.RemoteAddr),
			C.uint32_t(remoteInfo.RKey),
			C.uint32_t(n)) != 0 {
			fmt.Fprintln(os.Stderr, "ibv_post_send RDMA_WRITE failed")
			os.Exit(1)
		}
		if completionStatus := C.wait_send_completion(cq); completionStatus != 0 {
			fmt.Fprintf(os.Stderr, "RDMA write completion failed (status %d)\n", int(completionStatus))
			os.Exit(1)
		}

		readyBuf := make([]byte, 4)
		binary.NativeEndian.PutUint32(readyBuf, 1)
		if err := sendAll(conn, readyBuf); err != nil {
			fmt.Fprintf(os.Stderr, "send ready signal: %v\n", err)
			os.Exit(1)
		}

		ackBuf := make([]byte, 4)
		if err := recvAll(conn, ackBuf); err != nil {
			fmt.Fprintf(os.Stderr, "recv ack: %v\n", err)
			os.Exit(1)
		}
		if binary.NativeEndian.Uint32(ackBuf) != 1 {
			fmt.Fprintln(os.Stderr, "receiver rejected chunk")
			os.Exit(1)
		}

		transferred += n
		if !quiet && verbose {
			percent := 100.0 * float64(transferred) / float64(fileSizeBytes)
			fmt.Printf("Transferred %d/%d bytes (%.2f%%)\n", transferred, fileSizeBytes, percent)
		}
	}

	if summary || verbose {
		fmt.Printf("Transferred %d/%d bytes (100.00%%)\n", fileSizeBytes, fileSizeBytes)
	}
	if !quiet {
		fmt.Printf("Transfer complete: %d bytes sent successfully\n", fileSizeBytes)
	}

	elapsed := time.Since(start)
	throughput := float64(fileSizeBytes) / elapsed.Seconds() / 1024 / 1024 / 1024 * 8

	fmt.Printf("File sent successfully! ✅\n")
	fmt.Printf("Time:       %v\n", elapsed)
	fmt.Printf("Throughput: %.2f Gbps\n", throughput)
}

func marshalConnInfo(info connInfo) []byte {
	buf := make([]byte, 42)
	binary.NativeEndian.PutUint32(buf[0:4], info.QPNum)
	binary.NativeEndian.PutUint16(buf[4:6], info.LID)
	copy(buf[6:22], info.GID[:])
	binary.NativeEndian.PutUint32(buf[22:26], info.RKey)
	binary.NativeEndian.PutUint64(buf[26:34], info.RemoteAddr)
	binary.NativeEndian.PutUint64(buf[34:42], info.FileSize)
	return buf
}

func unmarshalConnInfo(data []byte) connInfo {
	var info connInfo
	info.QPNum = binary.NativeEndian.Uint32(data[0:4])
	info.LID = binary.NativeEndian.Uint16(data[4:6])
	copy(info.GID[:], data[6:22])
	info.RKey = binary.NativeEndian.Uint32(data[22:26])
	info.RemoteAddr = binary.NativeEndian.Uint64(data[26:34])
	info.FileSize = binary.NativeEndian.Uint64(data[34:42])
	return info
}

func marshalChunkMsg(msg chunkMsg) []byte {
	buf := make([]byte, 12)
	binary.NativeEndian.PutUint64(buf[0:8], msg.Offset)
	binary.NativeEndian.PutUint32(buf[8:12], msg.Length)
	return buf
}
