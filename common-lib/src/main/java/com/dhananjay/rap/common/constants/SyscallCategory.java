package com.dhananjay.rap.common.constants;

import java.util.Map;
import java.util.Set;

public final class SyscallCategory {

    private SyscallCategory() {}

    public static final String FILE_IO = "FILE_IO";
    public static final String NETWORK = "NETWORK";
    public static final String PROCESS = "PROCESS";
    public static final String MEMORY = "MEMORY";
    public static final String IPC = "IPC";
    public static final String SECURITY = "SECURITY";
    public static final String OTHER = "OTHER";

    private static final Map<String, Set<String>> CATEGORY_MAP = Map.of(
            FILE_IO, Set.of("read", "write", "open", "openat", "close", "stat", "fstat",
                    "lstat", "lseek", "pread64", "pwrite64", "readv", "writev",
                    "access", "rename", "mkdir", "rmdir", "unlink", "chmod", "chown"),
            NETWORK, Set.of("socket", "connect", "accept", "accept4", "bind", "listen",
                    "sendto", "recvfrom", "sendmsg", "recvmsg", "shutdown",
                    "getsockname", "getpeername", "setsockopt", "getsockopt"),
            PROCESS, Set.of("execve", "execveat", "clone", "clone3", "fork", "vfork",
                    "wait4", "waitid", "kill", "exit", "exit_group", "getpid",
                    "getppid", "setuid", "setgid", "prctl"),
            MEMORY, Set.of("mmap", "munmap", "mprotect", "brk", "madvise", "mlock",
                    "munlock", "mremap"),
            IPC, Set.of("pipe", "pipe2", "eventfd", "eventfd2", "signalfd",
                    "timerfd_create", "epoll_create", "epoll_ctl", "epoll_wait"),
            SECURITY, Set.of("ptrace", "mount", "umount2", "chroot", "capget",
                    "capset", "seccomp", "bpf", "perf_event_open")
    );

    public static String categorize(String syscall) {
        if (syscall == null) return OTHER;
        String lower = syscall.toLowerCase();
        for (Map.Entry<String, Set<String>> entry : CATEGORY_MAP.entrySet()) {
            if (entry.getValue().contains(lower)) {
                return entry.getKey();
            }
        }
        return OTHER;
    }
}
