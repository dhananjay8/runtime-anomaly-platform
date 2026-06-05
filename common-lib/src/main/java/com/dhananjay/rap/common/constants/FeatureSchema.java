package com.dhananjay.rap.common.constants;

public final class FeatureSchema {

    private FeatureSchema() {}

    public static final String SCHEMA_RESOURCE = "feature-schema.json";

    public static final String[] FEATURE_NAMES = {
            "syscall_read_freq",
            "syscall_write_freq",
            "syscall_open_freq",
            "syscall_close_freq",
            "syscall_exec_freq",
            "syscall_connect_freq",
            "syscall_accept_freq",
            "syscall_mmap_freq",
            "unique_process_count",
            "unique_syscall_count",
            "process_creation_rate",
            "unique_dest_ips",
            "unique_dest_ports",
            "network_event_ratio",
            "syscall_entropy",
            "process_entropy",
            "privileged_event_ratio",
            "uid_zero_ratio"
    };

    public static final String[] TRACKED_SYSCALLS = {
            "read", "write", "open", "close", "execve", "connect", "accept", "mmap"
    };

    public static final int FEATURE_COUNT = FEATURE_NAMES.length;
    public static final String SCHEMA_VERSION = "1.0";
}
