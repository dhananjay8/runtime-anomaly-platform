package com.dhananjay.rap.feature.engine;

import com.dhananjay.rap.common.constants.FeatureSchema;
import com.dhananjay.rap.common.event.FeatureVector;
import com.dhananjay.rap.common.event.ProcessedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FeatureExtractor {

    private static final String[] TRACKED_SYSCALLS = FeatureSchema.TRACKED_SYSCALLS;
    private static final String[] FEATURE_NAMES = FeatureSchema.FEATURE_NAMES;

    public FeatureVector extract(String containerId, List<ProcessedEvent> events,
                                  Instant windowStart, Instant windowEnd) {
        long totalEvents = events.size();
        if (totalEvents == 0) {
            return buildEmptyVector(containerId, windowStart, windowEnd);
        }

        Map<String, Long> syscallCounts = events.stream()
                .filter(e -> e.getSyscall() != null)
                .collect(Collectors.groupingBy(ProcessedEvent::getSyscall, Collectors.counting()));

        // Syscall frequency features (normalized by total events)
        double readFreq = syscallCounts.getOrDefault("read", 0L) / (double) totalEvents;
        double writeFreq = syscallCounts.getOrDefault("write", 0L) / (double) totalEvents;
        double openFreq = syscallCounts.getOrDefault("open", 0L) / (double) totalEvents;
        double closeFreq = syscallCounts.getOrDefault("close", 0L) / (double) totalEvents;
        double execFreq = syscallCounts.getOrDefault("execve", 0L) / (double) totalEvents;
        double connectFreq = syscallCounts.getOrDefault("connect", 0L) / (double) totalEvents;
        double acceptFreq = syscallCounts.getOrDefault("accept", 0L) / (double) totalEvents;
        double mmapFreq = syscallCounts.getOrDefault("mmap", 0L) / (double) totalEvents;

        // Process diversity features
        Set<String> uniqueProcesses = events.stream()
                .map(ProcessedEvent::getProcessName)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        int uniqueProcessCount = uniqueProcesses.size();

        Set<String> uniqueSyscalls = events.stream()
                .map(ProcessedEvent::getSyscall)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        int uniqueSyscallCount = uniqueSyscalls.size();

        long execveCount = syscallCounts.getOrDefault("execve", 0L) +
                syscallCounts.getOrDefault("clone", 0L) +
                syscallCounts.getOrDefault("fork", 0L);
        double windowSeconds = java.time.Duration.between(windowStart, windowEnd).getSeconds();
        double processCreationRate = windowSeconds > 0 ? execveCount / windowSeconds : 0;

        // Network features
        Set<String> destIps = events.stream()
                .map(ProcessedEvent::getDestinationIp)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        int uniqueDestIps = destIps.size();

        Set<Integer> destPorts = events.stream()
                .map(ProcessedEvent::getPort)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        int uniqueDestPorts = destPorts.size();

        long networkEvents = events.stream()
                .filter(e -> Boolean.TRUE.equals(e.getIsNetworkEvent()))
                .count();
        double networkEventRatio = networkEvents / (double) totalEvents;

        // Entropy features
        double syscallEntropy = calculateEntropy(syscallCounts, totalEvents);
        Map<String, Long> processCounts = events.stream()
                .filter(e -> e.getProcessName() != null)
                .collect(Collectors.groupingBy(ProcessedEvent::getProcessName, Collectors.counting()));
        double processEntropy = calculateEntropy(processCounts, totalEvents);

        // Privilege features
        long privilegedEvents = events.stream()
                .filter(e -> Boolean.TRUE.equals(e.getIsPrivileged()))
                .count();
        double privilegedEventRatio = privilegedEvents / (double) totalEvents;

        long uidZeroEvents = events.stream()
                .filter(e -> e.getUid() != null && e.getUid() == 0)
                .count();
        double uidZeroRatio = uidZeroEvents / (double) totalEvents;

        // Build feature array
        double[] features = {
                readFreq, writeFreq, openFreq, closeFreq, execFreq,
                connectFreq, acceptFreq, mmapFreq,
                uniqueProcessCount, uniqueSyscallCount, processCreationRate,
                uniqueDestIps, uniqueDestPorts, networkEventRatio,
                syscallEntropy, processEntropy,
                privilegedEventRatio, uidZeroRatio
        };

        int windowDuration = (int) windowSeconds;

        return FeatureVector.builder()
                .vectorId(UUID.randomUUID().toString())
                .containerId(containerId)
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .windowDurationSeconds(windowDuration)
                .totalEvents(totalEvents)
                .syscallReadFreq(readFreq)
                .syscallWriteFreq(writeFreq)
                .syscallOpenFreq(openFreq)
                .syscallCloseFreq(closeFreq)
                .syscallExecFreq(execFreq)
                .syscallConnectFreq(connectFreq)
                .syscallAcceptFreq(acceptFreq)
                .syscallMmapFreq(mmapFreq)
                .uniqueProcessCount(uniqueProcessCount)
                .uniqueSyscallCount(uniqueSyscallCount)
                .processCreationRate(processCreationRate)
                .uniqueDestIps(uniqueDestIps)
                .uniqueDestPorts(uniqueDestPorts)
                .networkEventRatio(networkEventRatio)
                .syscallEntropy(syscallEntropy)
                .processEntropy(processEntropy)
                .privilegedEventRatio(privilegedEventRatio)
                .uidZeroRatio(uidZeroRatio)
                .features(features)
                .featureNames(FEATURE_NAMES)
                .syscallFrequencies(syscallCounts)
                .build();
    }

    private double calculateEntropy(Map<String, Long> counts, long total) {
        if (total == 0) return 0.0;
        double entropy = 0.0;
        for (long count : counts.values()) {
            if (count > 0) {
                double p = count / (double) total;
                entropy -= p * Math.log(p) / Math.log(2);
            }
        }
        return entropy;
    }

    private FeatureVector buildEmptyVector(String containerId, Instant windowStart, Instant windowEnd) {
        return FeatureVector.builder()
                .vectorId(UUID.randomUUID().toString())
                .containerId(containerId)
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .windowDurationSeconds(0)
                .totalEvents(0L)
                .features(new double[FEATURE_NAMES.length])
                .featureNames(FEATURE_NAMES)
                .syscallFrequencies(Collections.emptyMap())
                .build();
    }
}
