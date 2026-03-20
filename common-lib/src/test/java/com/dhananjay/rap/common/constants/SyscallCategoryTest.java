package com.dhananjay.rap.common.constants;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SyscallCategoryTest {

    @Test
    void shouldCategorizeFileIOSyscalls() {
        assertThat(SyscallCategory.categorize("read")).isEqualTo(SyscallCategory.FILE_IO);
        assertThat(SyscallCategory.categorize("write")).isEqualTo(SyscallCategory.FILE_IO);
        assertThat(SyscallCategory.categorize("open")).isEqualTo(SyscallCategory.FILE_IO);
        assertThat(SyscallCategory.categorize("close")).isEqualTo(SyscallCategory.FILE_IO);
        assertThat(SyscallCategory.categorize("openat")).isEqualTo(SyscallCategory.FILE_IO);
    }

    @Test
    void shouldCategorizeNetworkSyscalls() {
        assertThat(SyscallCategory.categorize("connect")).isEqualTo(SyscallCategory.NETWORK);
        assertThat(SyscallCategory.categorize("accept")).isEqualTo(SyscallCategory.NETWORK);
        assertThat(SyscallCategory.categorize("bind")).isEqualTo(SyscallCategory.NETWORK);
        assertThat(SyscallCategory.categorize("socket")).isEqualTo(SyscallCategory.NETWORK);
    }

    @Test
    void shouldCategorizeProcessSyscalls() {
        assertThat(SyscallCategory.categorize("execve")).isEqualTo(SyscallCategory.PROCESS);
        assertThat(SyscallCategory.categorize("clone")).isEqualTo(SyscallCategory.PROCESS);
        assertThat(SyscallCategory.categorize("fork")).isEqualTo(SyscallCategory.PROCESS);
        assertThat(SyscallCategory.categorize("kill")).isEqualTo(SyscallCategory.PROCESS);
    }

    @Test
    void shouldCategorizeMemorySyscalls() {
        assertThat(SyscallCategory.categorize("mmap")).isEqualTo(SyscallCategory.MEMORY);
        assertThat(SyscallCategory.categorize("mprotect")).isEqualTo(SyscallCategory.MEMORY);
        assertThat(SyscallCategory.categorize("brk")).isEqualTo(SyscallCategory.MEMORY);
    }

    @Test
    void shouldCategorizeSecuritySyscalls() {
        assertThat(SyscallCategory.categorize("ptrace")).isEqualTo(SyscallCategory.SECURITY);
        assertThat(SyscallCategory.categorize("mount")).isEqualTo(SyscallCategory.SECURITY);
        assertThat(SyscallCategory.categorize("bpf")).isEqualTo(SyscallCategory.SECURITY);
        assertThat(SyscallCategory.categorize("seccomp")).isEqualTo(SyscallCategory.SECURITY);
    }

    @Test
    void shouldReturnOtherForUnknownSyscalls() {
        assertThat(SyscallCategory.categorize("unknown_syscall")).isEqualTo(SyscallCategory.OTHER);
        assertThat(SyscallCategory.categorize("custom")).isEqualTo(SyscallCategory.OTHER);
    }

    @Test
    void shouldHandleNullInput() {
        assertThat(SyscallCategory.categorize(null)).isEqualTo(SyscallCategory.OTHER);
    }

    @Test
    void shouldBeCaseInsensitive() {
        assertThat(SyscallCategory.categorize("READ")).isEqualTo(SyscallCategory.FILE_IO);
        assertThat(SyscallCategory.categorize("Connect")).isEqualTo(SyscallCategory.NETWORK);
        assertThat(SyscallCategory.categorize("EXECVE")).isEqualTo(SyscallCategory.PROCESS);
    }
}
