package com.opentable.kafka.util;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.concurrent.OTExecutors;
import com.opentable.metrics.AtomicLongGauge;
import com.opentable.metrics.graphite.MetricSets;

/**
 * Class that monitors consumer groups consuming from topics; registers its metrics with a metric registry.
 *
 * Runs a thread that polls the Kafka broker; use {@link #start()} and {@link #stop()} to spin up and down.
 *
 * The metric namespace is as follows; each metric is a {@link com.codahale.metrics.Gauge<Long>}.
 *
 * <p>
 * {@code <metric prefix>.<topic>.{partition.%d,total}.{size,offset,lag}}
 *
 * <p>
 * Size is the maximum offset of the topic (this is independent of any consumer). Offset is the current offset of the
 * consumer. Lag is the size minus the offset. Totals are summed across the partitions.
 * NB: Since the queries for the topic sizes and consumer offsets are necessarily separate, they race; this means that
 * lag can possibly be negative.
 *
 * <p>
 * In addition to these gauges, lag distribution {@link Histogram}s are also available as follows.
 *
 * <p>
 * {@code <metric prefix>.<topic>.total.lag-distribution}
 *
 * <p>
 * Future work: Have alternate constructor in which you don't specify any topics, and the class uses the
 * {@link OffsetMonitor} to dynamically register metrics based on the topics that the consumer group is consuming.
 *
 * <p>
 * Future work: Have alternate constructor in which you don't specify any groups either, and the class uses the
 * {@link OffsetMonitor} to dynamically register metrics based on all consumer groups' topic consumption.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class OffsetMetrics implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetMetrics.class);

    private final String metricPrefix;
    private final MetricRegistry metricRegistry;
    private final String groupId;
    private final Collection<String> topics;
    private final Duration pollPeriod;
    private final OffsetMonitor monitor;
    private final Map<String, Metric> metricMap;
    private final ScheduledExecutorService exec;

    public static OffsetMetricsBuilder builder(
            final String metricPrefix,
            final MetricRegistry metricRegistry,
            final String groupId,
            final String brokerList
    ) {
        return new OffsetMetricsBuilder(
                metricPrefix,
                metricRegistry,
                groupId,
                brokerList
        );
    }

    OffsetMetrics(
            final String metricPrefix,
            final MetricRegistry metricRegistry,
            final String groupId,
            final String brokerList,
            final Collection<String> topics,
            final Supplier<Reservoir> reservoirSupplier,
            final Duration pollPeriod) {
        Preconditions.checkArgument(metricPrefix != null, "null metric prefix");
        Preconditions.checkArgument(!topics.isEmpty(), "no topics");
        this.metricPrefix = metricPrefix;
        this.metricRegistry = metricRegistry;
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
                builder.put(partitionName(topic, part, "size"), new AtomicLongGauge());
                builder.put(partitionName(topic, part, "offset"), new AtomicLongGauge());
                builder.put(partitionName(topic, part, "lag"), new AtomicLongGauge());
            }
            builder.put(totalName(topic, "size"), new AtomicLongGauge());
            builder.put(totalName(topic, "offset"), new AtomicLongGauge());
            builder.put(totalName(topic, "lag"), new AtomicLongGauge());

            builder.put(totalName(topic, "lag-distribution"), new Histogram(reservoirSupplier.get()));
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

    private String partitionName(final String topic, final int partition, final String name) {
        return String.format("%s.%s.partition.%d.%s", metricPrefix, topic, partition, name);
    }

    private String totalName(final String topic, final String name) {
        return String.format("%s.%s.total.%s", metricPrefix, topic, name);
    }

    @VisibleForTesting
    Map<String, Metric> getMetrics() {
        return metricMap;
    }

    @PostConstruct
    public void start() {
        metricRegistry.registerAll(() -> metricMap);
        exec.scheduleAtFixedRate(this::poll, 0, pollPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop() {
        MetricSets.removeAll(metricRegistry, () -> metricMap);
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
        sizes.forEach((part, size) -> gauge(partitionName(topic, part, "size")).set(size));
        gauge(totalName(topic, "size")).set(sumValues(sizes));

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

        offsets.forEach((part, off) -> gauge(partitionName(topic, part, "offset")).set(off));
        lag    .forEach((part, off) -> {
            gauge(partitionName(topic, part, "lag")).set(off);
            ((Histogram) metricMap.get(totalName(topic, "lag-distribution"))).update(off);
        });

        gauge(totalName(topic, "offset")).set(sumValues(offsets));
        gauge(totalName(topic, "lag")).set(sumValues(lag));
    }

    private AtomicLongGauge gauge(final String name) {
        return (AtomicLongGauge) metricMap.get(name);
    }

    private static long sumValues(final Map<?, Long> map) {
        return map.values().stream().mapToLong(size -> size).sum();
    }
}
