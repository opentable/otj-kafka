package com.opentable.kafka.util;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import javax.annotation.concurrent.GuardedBy;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProgressRestoreListener implements StateRestoreListener, StateListener {
    private static final Logger LOG = LoggerFactory.getLogger(LogProgressRestoreListener.class);

    @GuardedBy("this")
    private final Map<String, Partitions> restoreState = new LinkedHashMap<>();

    @GuardedBy("this")
    private Instant lastPrint = Instant.now();
    private Instant restoreStart = Instant.now();

    private final StateListener delegate;

    public LogProgressRestoreListener() {
        this(null);
    }

    public LogProgressRestoreListener(StateListener delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onChange(State newState, State oldState) {
        if (delegate != null) {
            delegate.onChange(newState, oldState);
        }
        if (newState == State.REBALANCING) {
            LOG.info("Marking beginning of state restore operation");
            restoreStart = Instant.now();
        } else if (newState == State.RUNNING && restoreStart != null) {
            LOG.info("Marking end of restore operation, took {}", Duration.between(restoreStart, Instant.now()));
            restoreStart = null;
        }
    }

    @Override
    public synchronized void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        restoreState.computeIfAbsent(topicPartition.topic(), t -> {
            LOG.info("Begin restore topic '{}' into store '{}'", t, storeName);
            return new Partitions();
        }).onRestoreStart(topicPartition, startingOffset, endingOffset);
    }

    @Override
    public synchronized void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        final String topic = topicPartition.topic();
        final Partitions state = restoreState.get(topic);
        if (state.onRestoreEnd(topicPartition)) {
            LOG.trace("Completed restore of topic '{}'({}) into store '{}'", topic, totalRestored, storeName);
        }
    }

    @Override
    public synchronized void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        restoreState.get(topicPartition.topic()).onRestore(topicPartition, batchEndOffset);
        final Instant now = Instant.now();
        long biggestPartition = 0;
        if (Duration.between(lastPrint, now).getSeconds() <= 30) {
            return;
        }
        long offsetsSoFar = 0;
        long offsetsTotal = 0;
        for (Entry<String, Partitions> tp : restoreState.entrySet()) {
            final Partitions parts = tp.getValue();
            for (int p = 0; p < parts.offset.length; p++) {
                offsetsSoFar += parts.offset[p] - parts.start[p];
                long partitionLen = parts.end[p] - parts.start[p];
                biggestPartition = Math.max(partitionLen, biggestPartition);
                offsetsTotal += partitionLen > 0 ? partitionLen : biggestPartition;
            }
        }
        final double ratio = ((double) offsetsSoFar) / offsetsTotal;
        final Duration eta = Duration.ofMillis((long) (Duration.between(restoreStart, now).toMillis() / ratio));
        LOG.info("Currently restoring {} topics / {} topic-partitions, {} offsets of {}, {}, since {} ETA {}",
                restoreState.values().stream().filter(Partitions::isRestoring).count(),
                restoreState.values().stream().mapToInt(Partitions::countRestoring).sum(),
                offsetsSoFar,
                offsetsTotal,
                String.format("%.2f%%", ratio * 100.0d),
                restoreStart,
                eta);
        lastPrint = now;
    }

    static class Partitions {
        // NB this assumes partitions start at 0 and are consecutive.
        long[] start = new long[0];
        long[] offset = new long[0];
        long[] end = new long[0];

        void onRestoreStart(TopicPartition topicPartition, long startingOffset, long endingOffset) {
            final int partition = ensurePartition(topicPartition);
            start[partition] = offset[partition] = startingOffset;
            end[partition] = endingOffset;
        }

        void onRestore(TopicPartition topicPartition, long batchEndOffset) {
            offset[topicPartition.partition()] = batchEndOffset;
        }

        boolean isRestoring() {
            return countRestoring() > 0;
        }

        int countRestoring() {
            int restoring = 0;
            for (int i = 0; i < offset.length; i++) {
                if (offset[i] != end[i]) {
                    restoring++;
                }
            }
            return restoring;
        }

        boolean onRestoreEnd(TopicPartition topicPartition) {
            final int partition = topicPartition.partition();
            offset[partition] = end[partition];
            return IntStream.range(0, offset.length)
                    .noneMatch(p -> offset[p] != end[p]);
        }

        private int ensurePartition(TopicPartition topicPartition) {
            final int partition = topicPartition.partition();
            final int partitions = partition + 1;
            if (offset.length < partitions) {
                start = Arrays.copyOf(start, partitions);
                offset = Arrays.copyOf(offset, partitions);
                end = Arrays.copyOf(end, partitions);
            }
            return partition;
        }
    }
}
