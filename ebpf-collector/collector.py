#!/usr/bin/env python3
"""
eBPF Runtime Event Collector Agent

Captures syscalls, process execution, and network connections from
containerized workloads using BCC (BPF Compiler Collection) and
streams JSON events to Kafka topic: runtime-events.

Requires Linux kernel >= 4.15 with BPF support.
Must run as root (CAP_SYS_ADMIN).
"""

import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import click
import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = structlog.get_logger()

# eBPF programs are imported conditionally (Linux-only)
try:
    from bcc import BPF
    HAS_BCC = True
except ImportError:
    HAS_BCC = False
    logger.warning("BCC not available - running in simulation mode")


# ============================================================================
# eBPF Programs (C code compiled at runtime by BCC)
# ============================================================================

SYSCALL_TRACE_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <linux/sched.h>
#include <linux/nsproxy.h>
#include <linux/pid_namespace.h>

struct syscall_event_t {
    u64 timestamp_ns;
    u32 pid;
    u32 ppid;
    u32 uid;
    u32 syscall_id;
    s64 return_value;
    char comm[64];
    char container_id[128];
};

BPF_PERF_OUTPUT(syscall_events);
BPF_HASH(container_map, u32, char[128]);

static inline int get_container_id(char *buf, int buf_size) {
    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    // Read cgroup name to extract container ID
    // Simplified: read from /proc/self/cgroup in userspace
    buf[0] = '\0';
    return 0;
}

TRACEPOINT_PROBE(raw_syscalls, sys_exit) {
    struct syscall_event_t event = {};

    event.timestamp_ns = bpf_ktime_get_ns();
    event.pid = bpf_get_current_pid_tgid() >> 32;
    event.uid = bpf_get_current_uid_gid() & 0xFFFFFFFF;
    event.syscall_id = args->id;
    event.return_value = args->ret;

    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    event.ppid = task->real_parent->tgid;

    bpf_get_current_comm(&event.comm, sizeof(event.comm));

    syscall_events.perf_submit(args, &event, sizeof(event));
    return 0;
}
"""

NETWORK_TRACE_PROGRAM = r"""
#include <uapi/linux/ptrace.h>
#include <net/sock.h>
#include <bcc/proto.h>

struct net_event_t {
    u64 timestamp_ns;
    u32 pid;
    u32 uid;
    u32 daddr;
    u32 saddr;
    u16 dport;
    u16 sport;
    u8 protocol;
    char comm[64];
};

BPF_PERF_OUTPUT(net_events);

int trace_connect_entry(struct pt_regs *ctx, struct sock *sk) {
    struct net_event_t event = {};

    event.timestamp_ns = bpf_ktime_get_ns();
    event.pid = bpf_get_current_pid_tgid() >> 32;
    event.uid = bpf_get_current_uid_gid() & 0xFFFFFFFF;

    // Read socket address info
    event.daddr = sk->__sk_common.skc_daddr;
    event.saddr = sk->__sk_common.skc_rcv_saddr;
    event.dport = sk->__sk_common.skc_dport;
    event.sport = sk->__sk_common.skc_num;
    event.protocol = sk->sk_protocol;

    bpf_get_current_comm(&event.comm, sizeof(event.comm));

    net_events.perf_submit(ctx, &event, sizeof(event));
    return 0;
}
"""

# ============================================================================
# Syscall ID to Name Mapping (x86_64)
# ============================================================================

SYSCALL_NAMES = {
    0: "read", 1: "write", 2: "open", 3: "close", 4: "stat",
    5: "fstat", 6: "lstat", 7: "poll", 8: "lseek", 9: "mmap",
    10: "mprotect", 11: "munmap", 12: "brk", 14: "ioctl",
    17: "pread64", 18: "pwrite64", 19: "readv", 20: "writev",
    21: "access", 22: "pipe", 23: "select", 32: "dup", 33: "dup2",
    39: "getpid", 41: "socket", 42: "connect", 43: "accept",
    44: "sendto", 45: "recvfrom", 46: "sendmsg", 47: "recvmsg",
    48: "shutdown", 49: "bind", 50: "listen", 51: "getsockname",
    52: "getpeername", 56: "clone", 57: "fork", 58: "vfork",
    59: "execve", 60: "exit", 61: "wait4", 62: "kill",
    72: "fcntl", 78: "getdents", 80: "chdir", 82: "rename",
    83: "mkdir", 84: "rmdir", 85: "creat", 87: "unlink",
    90: "chmod", 92: "chown", 101: "ptrace", 160: "setrlimit",
    165: "mount", 166: "umount2", 167: "swapon",
    200: "tkill", 231: "exit_group", 257: "openat",
    262: "newfstatat", 288: "accept4", 302: "prlimit64",
    317: "seccomp", 321: "bpf", 322: "execveat",
    435: "clone3",
}


# ============================================================================
# Container ID Resolution
# ============================================================================

def resolve_container_id(pid: int) -> Optional[str]:
    """Resolve container ID from /proc/<pid>/cgroup."""
    try:
        cgroup_path = f"/proc/{pid}/cgroup"
        if not os.path.exists(cgroup_path):
            return None
        with open(cgroup_path, "r") as f:
            for line in f:
                parts = line.strip().split("/")
                for part in reversed(parts):
                    # Docker/Podman container IDs are 64-char hex strings
                    if len(part) == 64 and all(c in "0123456789abcdef" for c in part):
                        return part[:12]
                    # containerd format: cri-containerd-<id>.scope
                    if part.startswith("cri-containerd-"):
                        return part.replace("cri-containerd-", "").replace(".scope", "")[:12]
                    # Podman format
                    if part.startswith("libpod-"):
                        return part.replace("libpod-", "").replace(".scope", "")[:12]
    except (FileNotFoundError, PermissionError):
        pass
    return None


def int_to_ip(addr: int) -> str:
    """Convert integer IP address to dotted notation."""
    return f"{addr & 0xFF}.{(addr >> 8) & 0xFF}.{(addr >> 16) & 0xFF}.{(addr >> 24) & 0xFF}"


# ============================================================================
# Kafka Producer
# ============================================================================

class EventPublisher:
    """Publishes runtime events to Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            retry_backoff_ms=1000,
            batch_size=16384,
            linger_ms=10,
            compression_type="lz4",
            max_in_flight_requests_per_connection=5,
        )
        logger.info("Kafka producer initialized", servers=bootstrap_servers, topic=topic)

    def publish(self, event: dict):
        """Publish a single event to Kafka."""
        key = event.get("container_id", "unknown")
        try:
            future = self.producer.send(self.topic, key=key, value=event)
            future.add_callback(lambda meta: logger.debug(
                "Event published", topic=meta.topic, partition=meta.partition, offset=meta.offset
            ))
            future.add_errback(lambda exc: logger.error(
                "Failed to publish event", error=str(exc)
            ))
        except KafkaError as e:
            logger.error("Kafka send error", error=str(e))

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")


# ============================================================================
# eBPF Collector
# ============================================================================

class EbpfCollector:
    """Main eBPF collector that attaches probes and processes events."""

    def __init__(self, publisher: EventPublisher, filter_containers_only: bool = True):
        self.publisher = publisher
        self.filter_containers_only = filter_containers_only
        self.running = False
        self.event_count = 0
        self.bpf_syscall = None
        self.bpf_network = None

    def start(self):
        """Attach eBPF probes and start collecting events."""
        if not HAS_BCC:
            logger.error("BCC not available. Cannot start eBPF collector.")
            return

        logger.info("Attaching eBPF probes...")

        # Syscall tracing
        self.bpf_syscall = BPF(text=SYSCALL_TRACE_PROGRAM)
        self.bpf_syscall["syscall_events"].open_perf_buffer(self._handle_syscall_event)

        # Network tracing
        self.bpf_network = BPF(text=NETWORK_TRACE_PROGRAM)
        self.bpf_network.attach_kprobe(event="tcp_v4_connect", fn_name="trace_connect_entry")
        self.bpf_network["net_events"].open_perf_buffer(self._handle_network_event)

        self.running = True
        logger.info("eBPF probes attached. Collecting events...")

        while self.running:
            try:
                self.bpf_syscall.perf_buffer_poll(timeout=100)
                self.bpf_network.perf_buffer_poll(timeout=100)
            except KeyboardInterrupt:
                break

        self.stop()

    def stop(self):
        """Detach probes and clean up."""
        self.running = False
        self.publisher.flush()
        logger.info("Collector stopped", total_events=self.event_count)

    def _handle_syscall_event(self, cpu, data, size):
        """Process a syscall event from the eBPF perf buffer."""
        event = self.bpf_syscall["syscall_events"].event(data)

        container_id = resolve_container_id(event.pid)
        if self.filter_containers_only and not container_id:
            return

        syscall_name = SYSCALL_NAMES.get(event.syscall_id, f"unknown_{event.syscall_id}")

        runtime_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "container_id": container_id or "host",
            "pid": event.pid,
            "ppid": event.ppid,
            "syscall": syscall_name,
            "syscall_id": event.syscall_id,
            "process_name": event.comm.decode("utf-8", errors="replace"),
            "args": None,
            "return_value": event.return_value,
            "uid": event.uid,
            "hostname": os.uname().nodename,
        }

        self.publisher.publish(runtime_event)
        self.event_count += 1

    def _handle_network_event(self, cpu, data, size):
        """Process a network event from the eBPF perf buffer."""
        event = self.bpf_network["net_events"].event(data)

        container_id = resolve_container_id(event.pid)
        if self.filter_containers_only and not container_id:
            return

        runtime_event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "container_id": container_id or "host",
            "pid": event.pid,
            "syscall": "connect",
            "process_name": event.comm.decode("utf-8", errors="replace"),
            "destination_ip": int_to_ip(event.daddr),
            "source_ip": int_to_ip(event.saddr),
            "port": event.dport,
            "protocol": "TCP" if event.protocol == 6 else "UDP" if event.protocol == 17 else str(event.protocol),
            "uid": event.uid,
            "hostname": os.uname().nodename,
        }

        self.publisher.publish(runtime_event)
        self.event_count += 1


# ============================================================================
# Simulation Mode (for Mac / non-Linux development)
# ============================================================================

class SimulatedCollector:
    """Generates synthetic eBPF-like events for development/testing on non-Linux."""

    SAMPLE_CONTAINERS = [
        ("abc123def456", "nginx:1.25", "web-frontend"),
        ("789ghi012jkl", "redis:7.2", "cache-layer"),
        ("mno345pqr678", "python:3.11", "ml-worker"),
        ("stu901vwx234", "postgres:16", "database"),
        ("yz5678abcdef", "node:20", "api-gateway"),
    ]

    NORMAL_SYSCALLS = [
        ("read", 0), ("write", 1), ("open", 2), ("close", 3),
        ("stat", 4), ("fstat", 5), ("mmap", 9), ("mprotect", 10),
        ("brk", 12), ("openat", 257), ("poll", 7),
    ]

    ANOMALOUS_SYSCALLS = [
        ("ptrace", 101), ("mount", 165), ("execve", 59),
        ("clone", 56), ("bpf", 321), ("seccomp", 317),
    ]

    NORMAL_PROCESSES = ["nginx", "redis-server", "python3", "postgres", "node"]
    ANOMALOUS_PROCESSES = ["nc", "nmap", "curl", "wget", "sh", "bash", "chmod"]

    def __init__(self, publisher: EventPublisher, anomaly_rate: float = 0.05):
        self.publisher = publisher
        self.anomaly_rate = anomaly_rate
        self.running = False
        self.event_count = 0

    def start(self, events_per_second: int = 100):
        """Generate synthetic events at the specified rate."""
        import random

        self.running = True
        interval = 1.0 / events_per_second
        logger.info("Starting simulated collector", eps=events_per_second, anomaly_rate=self.anomaly_rate)

        while self.running:
            try:
                is_anomalous = random.random() < self.anomaly_rate
                container = random.choice(self.SAMPLE_CONTAINERS)

                if is_anomalous:
                    syscall_name, syscall_id = random.choice(self.ANOMALOUS_SYSCALLS)
                    process = random.choice(self.ANOMALOUS_PROCESSES)
                else:
                    syscall_name, syscall_id = random.choice(self.NORMAL_SYSCALLS)
                    process = random.choice(self.NORMAL_PROCESSES)

                event = {
                    "event_id": str(uuid.uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "container_id": container[0],
                    "pid": random.randint(1000, 65535),
                    "ppid": random.randint(1, 999),
                    "syscall": syscall_name,
                    "syscall_id": syscall_id,
                    "process_name": process,
                    "args": None,
                    "return_value": 0 if random.random() > 0.1 else -1,
                    "uid": 0 if (is_anomalous and random.random() > 0.5) else random.randint(1000, 65534),
                    "hostname": "collector-sim-01",
                    "image_name": container[1],
                    "namespace": container[2],
                }

                # Add network info for connect syscalls
                if syscall_name == "connect" or (is_anomalous and random.random() > 0.7):
                    event["destination_ip"] = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
                    event["source_ip"] = f"10.0.{random.randint(0,255)}.{random.randint(1,254)}"
                    event["port"] = random.choice([80, 443, 8080, 3306, 5432, 6379, 9092, 22, 4444, 8888])
                    event["protocol"] = random.choice(["TCP", "UDP"])
                    event["syscall"] = "connect"

                self.publisher.publish(event)
                self.event_count += 1

                if self.event_count % 1000 == 0:
                    logger.info("Events generated", count=self.event_count)

                time.sleep(interval)

            except KeyboardInterrupt:
                break

        self.stop()

    def stop(self):
        self.running = False
        self.publisher.flush()
        logger.info("Simulated collector stopped", total_events=self.event_count)


# ============================================================================
# CLI Entry Point
# ============================================================================

@click.command()
@click.option("--kafka-servers", default="localhost:9092", help="Kafka bootstrap servers")
@click.option("--topic", default="runtime-events", help="Kafka topic to publish events to")
@click.option("--mode", type=click.Choice(["ebpf", "simulate"]), default="simulate",
              help="Collection mode: ebpf (requires Linux+root) or simulate (synthetic data)")
@click.option("--eps", default=100, type=int, help="Events per second (simulation mode)")
@click.option("--anomaly-rate", default=0.05, type=float, help="Anomaly injection rate (simulation mode)")
@click.option("--containers-only/--all-processes", default=True,
              help="Only capture container events (eBPF mode)")
def main(kafka_servers, topic, mode, eps, anomaly_rate, containers_only):
    """eBPF Runtime Event Collector Agent."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )

    logger.info("Starting collector", mode=mode, kafka=kafka_servers, topic=topic)

    publisher = EventPublisher(kafka_servers, topic)

    def signal_handler(sig, frame):
        logger.info("Shutdown signal received")
        if mode == "ebpf":
            collector.stop()
        else:
            sim_collector.stop()
        publisher.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if mode == "ebpf":
        if not HAS_BCC:
            logger.error("BCC not available. Install bcc-tools or use --mode simulate")
            sys.exit(1)
        if os.geteuid() != 0:
            logger.error("eBPF mode requires root. Run with sudo or use --mode simulate")
            sys.exit(1)
        collector = EbpfCollector(publisher, filter_containers_only=containers_only)
        collector.start()
    else:
        sim_collector = SimulatedCollector(publisher, anomaly_rate=anomaly_rate)
        sim_collector.start(events_per_second=eps)


if __name__ == "__main__":
    main()
