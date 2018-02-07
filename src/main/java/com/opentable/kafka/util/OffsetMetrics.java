package com.opentable.kafka.util;

import java.io.Closeable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.Refill;

import com.opentable.concurrent.OTExecutors;
import com.opentable.metrics.graphite.MetricSets;

/**
 * Class that monitors consumer groups consuming from topics; registers its metrics with a metric registry.
 *
 * Runs a thread that polls the Kafka broker; use {@link #start()} and {@link #stop()} to spin up and down. They are
 * annotated with post-construct and pre-destroy, respectively, in order to simplify Spring integration.
 *
 * The metric namespace is as follows; each metric is a {@link Gauge}&lt;{@link Long}&gt;, and can be {@code null}.
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
 * Use an offsets supplier if you are managing your own offsets.
 * Without an offsets supplier, this class will attempt to read consumer group offsets from the Kafka broker, and will
 * require the partitions for which offsets are stored to match the partitions for which size information is reported.
 * With an offsets supplier, this class will call it periodically. The offsets supplier's keys (partitions) must be a
 * subset of the keys (also partitions) of the size information reported by the broker.
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

    private final Bucket logLimitBucket = Bucket4j
            .builder()
            .addLimit(Bandwidth.classic(6, Refill.smooth(1, Duration.ofMinutes(10))))
            .build();

    private final String metricPrefix;
    private final MetricRegistry metricRegistry;
    private final Collection<String> topics;
    private final Duration pollPeriod;
    private final OffsetMonitor monitor;
    private final boolean expectKafkaManagedOffsets;
    /** Topic -> partition -> offset. */
    private final Function<String, Map<Integer, Long>> offsetsSupplier;
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
            final Duration pollPeriod,
            @Nullable
            final Function<String, Map<Integer, Long>> offsetsSupplier) {
        Preconditions.checkArgument(metricPrefix != null, "null metric prefix");
        Preconditions.checkArgument(!topics.isEmpty(), "no topics");
        this.metricPrefix = metricPrefix;
        this.metricRegistry = metricRegistry;
        this.topics = topics;
        this.pollPeriod = pollPeriod;
        monitor = new OffsetMonitor(groupId, brokerList);
        expectKafkaManagedOffsets = offsetsSupplier == null;
        if (expectKafkaManagedOffsets) {
            this.offsetsSupplier = topic -> monitor.getGroupOffsets(groupId, topic);
        } else {
            this.offsetsSupplier = offsetsSupplier;
        }

        final Collection<String> badTopics = new ArrayList<>();
        final ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
        topics.forEach(topic -> {
            final Map<Integer, Long> sizes = monitor.getTopicSizes(topic);
            if (sizes.isEmpty()) {
                badTopics.add(topic);
                return;
            }
            for (final int part : sizes.keySet()) {
                builder.put(partitionName(topic, part, "size"), new LongGauge());
                builder.put(partitionName(topic, part, "offset"), new LongGauge());
                builder.put(partitionName(topic, part, "lag"), new LongGauge());
            }
            builder.put(totalName(topic, "size"), new LongGauge());
            builder.put(totalName(topic, "offset"), new LongGauge());
            builder.put(totalName(topic, "lag"), new LongGauge());

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
        metricRegistry.registerAll(this::getMetrics);
        exec.scheduleAtFixedRate(this::poll, 0, pollPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop() {
        MetricSets.removeAll(metricRegistry, this::getMetrics);
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

        Map<Integer, Long> offsets = offsetsSupplier.apply(topic);
        final Map<Integer, Long> lags;
        if (offsets.isEmpty()) {
            // Consumer may not be consuming this topic yet (or consumer might not exist).
            // In case consumer existed previously, we set all offsets and lag to 0.
            offsets = lags = sizes
                    .keySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    part -> part,
                                    part -> 0L
                            )
                    );
        } else {
            if (expectKafkaManagedOffsets) {
                if (!sizes.keySet().equals(offsets.keySet())) {
                    if (logLimitBucket.tryConsume(1)) {
                        LOG.warn("sizes/offsets partitions do not match for topic {}: {}/{}",
                                topic, sizes.keySet(), offsets.keySet());
                    }
                    return;
                }
            } else {
                if (!sizes.keySet().containsAll(offsets.keySet())) {
                    if (logLimitBucket.tryConsume(1)) {
                        LOG.warn("offsets not subset of sizes for topic {}: {}/{}",
                                topic, offsets.keySet(), sizes.keySet());
                    }
                    return;
                }
            }
            final Map<Integer, Long> finalOffsets = offsets;
            lags = offsets
                    .keySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                    part -> part,
                                    part -> sizes.get(part) - finalOffsets.get(part)
                            )
                    );
        }

        // The keys of offsets is a non-empty subset of the keys of sizes, and the keys of offsets and lags are
        // identical.
        final Set<Integer> knownParts = offsets.keySet();
        final Set<Integer> unknownParts = Sets.difference(offsets.keySet(), knownParts);

        subMap(knownParts, sizes).forEach((part, size) -> gauge(partitionName(topic, part, "size")).set(size));
        unknownParts.forEach(              part        -> gauge(partitionName(topic, part, "size")).set(null));
        gauge(totalName(topic, "size")).set(sumValues(subMap(knownParts, sizes)));

        offsets     .forEach((part, off) -> gauge(partitionName(topic, part, "offset")).set(off));
        unknownParts.forEach( part       -> gauge(partitionName(topic, part, "offset")).set(null));
        gauge(totalName(topic, "offset")).set(sumValues(offsets));

        lags.forEach((part, lag) -> {
            gauge(partitionName(topic, part, "lag")).set(lag);
            ((Histogram) metricMap.get(totalName(topic, "lag-distribution"))).update(lag);
        });
        unknownParts.forEach(part -> gauge(partitionName(topic, part, "lag")).set(null));
        gauge(totalName(topic, "lag")).set(sumValues(lags));
    }

    private LongGauge gauge(final String name) {
        return (LongGauge) metricMap.get(name);
    }

    private static long sumValues(final Map<?, Long> map) {
        return map.values().stream().mapToLong(x -> x).sum();
    }

    /**
     * Sub-map of {@code map} whose keys are the elements of {@code keys} with corresponding values from {@code map}.
     */
    private static <K, V> Map<K, V> subMap(final Collection<K> keys, final Map<K, V> map) {
        return new AbstractMap<K, V>() {
            @Override
            public Set<Entry<K, V>> entrySet() {
                return new AbstractSet<Entry<K, V>>() {
                    @Override
                    public Iterator<Entry<K, V>> iterator() {
                        final Iterator<Entry<K, V>> itr = map.entrySet().iterator();
                        return new AbstractIterator<Entry<K, V>>() {
                            @Override
                            protected Entry<K, V> computeNext() {
                                while (itr.hasNext()) {
                                    final Entry<K, V> e = itr.next();
                                    if (keys.contains(e.getKey())) {
                                        return e;
                                    }
                                }
                                return endOfData();
                            }
                        };
                    }

                    @Override
                    public int size() {
                        return keys.size();
                    }
                };
            }
        };
    }

    @VisibleForTesting
    static class LongGauge implements Gauge<Long>, Counting {
        private final AtomicReference<Long> value = new AtomicReference<>();

        @Override
        public Long getValue() {
            return value.get();
        }

        @Override
        public long getCount() {
            return Optional.ofNullable(getValue()).orElse(0L);
        }

        private void set(final Long value) {
            this.value.set(value);
        }
    }
}
