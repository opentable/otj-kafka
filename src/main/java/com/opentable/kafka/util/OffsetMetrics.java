package com.opentable.kafka.util;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.concurrent.OTExecutors;
import com.opentable.metrics.AtomicLongGauge;

/**
 * {@link MetricSet} implementation that monitors consumer groups consuming from topics.
 *
 * Runs a thread that polls the Kafka broker; use {@link #start()} and {@link #stop()} to spin up and down.
 *
 * The metric namespace is as follows.
 *
 * <p>
 * {@code <topic>.{partition.%d,total}.{size,offset,lag}}
 *
 * <p>
 * Size is the maximum offset of the topic (this is independent of any consumer). Offset is the current offset of the
 * consumer. Lag is the size minus the offset. Totals are summed across the partitions.
 */
public class OffsetMetrics implements MetricSet, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetMetrics.class);

    private final String groupId;
    private final Collection<String> topics;
    private final Duration pollPeriod;
    private final OffsetMonitor monitor;
    private final Map<String, Metric> metricMap;
    private final ScheduledExecutorService exec;

    public OffsetMetrics(final String groupId, final String brokerList, final Collection<String> topics) {
        this(groupId, brokerList, topics, Duration.ofSeconds(10));
    }

    @VisibleForTesting
    OffsetMetrics(
            final String groupId,
            final String brokerList,
            final Collection<String> topics,
            final Duration pollPeriod) {
        Preconditions.checkArgument(!topics.isEmpty(), "no topics");
        this.groupId = groupId;
        this.topics = topics;
        this.pollPeriod = pollPeriod;
        monitor = new OffsetMonitor(groupId, brokerList);

        final Collection<String> badTopics = new ArrayList<>();
        final ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
        topics.forEach(topic -> {
            final Map<Integer, Long> sizes = monitor.getTopicSizes(topic);
            if (sizes.isEmpty()) {
                badTopics.add(topic);
                return;
            }
            for (final int part : sizes.keySet()) {
                builder.put(String.format("%s.partition.%d.size",   topic, part), new AtomicLongGauge());
                builder.put(String.format("%s.partition.%d.offset", topic, part), new AtomicLongGauge());
                builder.put(String.format("%s.partition.%d.lag",    topic, part), new AtomicLongGauge());
            }
            builder.put(String.format("%s.total.size",   topic), new AtomicLongGauge());
            builder.put(String.format("%s.total.offset", topic), new AtomicLongGauge());
            builder.put(String.format("%s.total.lag",    topic), new AtomicLongGauge());
        });

        if (!badTopics.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("bad topic%s: '%s'", badTopics.size() != 1 ? "s" : "", badTopics));
        }

        metricMap = builder.build();

        exec = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(String.format("%s-offset-metrics-%%d", groupId))
                        .build()
        );
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metricMap;
    }

    public void start() {
        exec.scheduleAtFixedRate(this::poll, 0, pollPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        try {
            OTExecutors.shutdownAndAwaitTermination(exec, Duration.ofSeconds(5));
        } catch (final InterruptedException e) {
            LOG.error("worker thread shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
        monitor.close();
    }

    @Override
    public void close() {
        stop();
    }

    private void poll() {
        topics.forEach(topic -> {
            try {
                pollUnsafe(topic);
            } catch (final Exception e) {
                LOG.error("caught exception for topic {}", topic, e);
            }
        });
    }

    private void pollUnsafe(final String topic) {
        final Map<Integer, Long> sizes = monitor.getTopicSizes(topic);
        sizes.forEach((part, size) -> gauge(String.format("%s.partition.%d.size", topic, part)).set(size));
        gauge(String.format("%s.total.size", topic)).set(sumValues(sizes));

        Map<Integer, Long> offsets = monitor.getGroupOffsets(groupId, topic);
        final Map<Integer, Long> lag;
        if (offsets.isEmpty()) {
            // Consumer may not be consuming this topic yet (or consumer might not exist).
            // In case consumer existed previously, we set all offsets and lag to 0.
            offsets = lag = sizes
                    .keySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    part -> part,
                                    part -> 0L
                            )
                    );
        } else {
            if (!sizes.keySet().equals(offsets.keySet())) {
                LOG.error("sizes/offsets partitions do not match for topic {}: {}/{}",
                        topic, sizes.keySet(), offsets.keySet());
                return;
            }
            final Map<Integer, Long> finalOffsets = offsets;
            lag = sizes
                    .keySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    part -> part,
                                    part -> sizes.get(part) - finalOffsets.get(part)
                            )
                    );
        }

        offsets.forEach((part, off) -> gauge(String.format("%s.partition.%d.offset", topic, part)).set(off));
        lag    .forEach((part, off) -> gauge(String.format("%s.partition.%d.lag",    topic, part)).set(off));

        gauge(String.format("%s.total.offset", topic)).set(sumValues(offsets));
        gauge(String.format("%s.total.lag",    topic)).set(sumValues(lag));
    }

    private AtomicLongGauge gauge(final String name) {
        return (AtomicLongGauge) metricMap.get(name);
    }

    private static long sumValues(final Map<?, Long> map) {
        return map.values().stream().mapToLong(size -> size).sum();
    }
}
