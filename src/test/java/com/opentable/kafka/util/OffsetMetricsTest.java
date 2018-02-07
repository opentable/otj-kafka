package com.opentable.kafka.util;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class OffsetMetricsTest {
    private static final String METRIC_NS = "foobar";

    @Test(timeout = 30_000, expected = IllegalArgumentException.class)
    public void testNoTopics() {
        try (ReadWriteRule rw = new ReadWriteRule()) {
            OffsetMetrics
                    .builder(METRIC_NS, new MetricRegistry(), rw.getGroupId(), rw.getBroker().getKafkaBrokerConnect())
                    .build();
        }
    }

    @Test(timeout = 30_000, expected = IllegalArgumentException.class)
    public void testMissingTopic() {
        try (ReadWriteRule rw = new ReadWriteRule()) {
            OffsetMetrics
                    .builder(METRIC_NS, new MetricRegistry(), rw.getGroupId(), rw.getBroker().getKafkaBrokerConnect())
                    .addTopics("no-topic-1")
                    .build();
        }
    }

    @Test(timeout = 60_000)
    public void testMetrics() throws InterruptedException {
        try (
                ReadWriteRule rw = new ReadWriteRule();
                OffsetMetrics metrics = OffsetMetrics
                        .builder(METRIC_NS, new MetricRegistry(), rw.getGroupId(), rw.getBroker().getKafkaBrokerConnect())
                        .addTopic(rw.getTopicName())
                        .withPollPeriod(Duration.ofMillis(500))
                        .build()
        ) {
            metrics.start();

            // Write and read some test records.
            final int n = 10;
            rw.writeTestRecords(1, n);
            rw.readTestRecords(n);

            // Ensure metrics true with records written and read.
            waitForMetric(rw, metrics, "size", n);
            waitForMetric(rw, metrics, "offset", n);
            waitForMetric(rw, metrics, "lag", 0);

            // Write some more, but don't read yet.
            final int more = 3;
            rw.writeTestRecords(n + 1, n + more);

            // Ensure metrics true up, including lag.
            waitForMetric(rw, metrics, "size", n + more);
            waitForMetric(rw, metrics, "lag", more);
            waitForMetric(rw, metrics, "offset", n);

            // Have consumer catch up.
            rw.readTestRecords(more);

            // Make sure lag goes back to zero and offset catches up.
            waitForMetric(rw, metrics, "lag", 0);
            waitForMetric(rw, metrics, "offset", n + more);
        }
    }

    @Test(timeout = 60_000)
    public void testOwnOffsets() throws InterruptedException {
        final Map<Integer, Long> offsets = ImmutableMap.of(
                0, 50L,
                1, 50L
        );
        final MetricRegistry metricRegistry = new MetricRegistry();
        try (
                ReadWriteRule rw = new ReadWriteRule(3);
                OffsetMetrics metrics = OffsetMetrics
                        .builder(METRIC_NS, metricRegistry, rw.getGroupId(), rw.getBroker().getKafkaBrokerConnect())
                        .addTopic(rw.getTopicName())
                        .withPollPeriod(Duration.ofMillis(500))
                        .withOffsetsSupplier(() -> offsets)
                        .build()
        ) {
            metrics.start();

            waitForMetric(rw, metrics, "size", 0);
            waitForMetric(rw, metrics, "offset", 50);
            waitForMetric(rw, metrics, "lag", -50);

            final String prefix = String.format("%s.%s.partition.", METRIC_NS, rw.getTopicName());
            final Set<Integer> partitions = new HashSet<>();
            metricRegistry.getMetrics().forEach((name, metric) -> {
                if (name.startsWith(prefix)) {
                    final OffsetMetrics.LongGauge gauge = (OffsetMetrics.LongGauge) metric;
                    final String partitionString = name.substring(prefix.length(), prefix.length() + 1);
                    final int partition = Integer.parseInt(partitionString);
                    if (gauge.getValue() != null) {
                        partitions.add(partition);
                    }
                }
            });
            Assertions.assertThat(partitions).containsExactly(0, 1);
        }
    }

    private void waitForMetric(
            final ReadWriteRule rw,
            final OffsetMetrics metrics,
            final String nameSuffix,
            final long value)
            throws InterruptedException {
        final String metricName = String.format("%s.%s.partition.0.%s", METRIC_NS, rw.getTopicName(), nameSuffix);
        while (true) {
            final Counting c = (Counting) metrics.getMetrics().get(metricName);
            if (c.getCount() == value) {
                break;
            }
            ReadWriteRule.loopSleep();
        }
    }
}
