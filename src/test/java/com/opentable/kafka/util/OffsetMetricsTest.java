package com.opentable.kafka.util;

import java.time.Duration;

import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricRegistry;

import org.junit.Rule;
import org.junit.Test;

public class OffsetMetricsTest {
    private static final String METRIC_NS = "foobar";

    @Rule
    public final ReadWriteRule rw = new ReadWriteRule();

    @Test(timeout = 30_000, expected = IllegalArgumentException.class)
    public void testNoTopics() {
        OffsetMetrics
                .builder(METRIC_NS, new MetricRegistry(), rw.getGroupId(), rw.getBroker().getKafkaBrokerConnect())
                .build();
    }

    @Test(timeout = 30_000, expected = IllegalArgumentException.class)
    public void testMissingTopic() {
        OffsetMetrics
                .builder(METRIC_NS, new MetricRegistry(), rw.getGroupId(), rw.getBroker().getKafkaBrokerConnect())
                .addTopics("no-topic-1")
                .build();
    }

    @Test(timeout = 60_000)
    public void testMetrics() throws InterruptedException {
        try (OffsetMetrics metrics = OffsetMetrics
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
            waitForMetric(metrics, "size", n);
            waitForMetric(metrics, "offset", n);
            waitForMetric(metrics, "lag", 0);

            // Write some more, but don't read yet.
            final int more = 3;
            rw.writeTestRecords(n + 1, n + more);

            // Ensure metrics true up, including lag.
            waitForMetric(metrics, "size", n + more);
            waitForMetric(metrics, "lag", more);
            waitForMetric(metrics, "offset", n);

            // Have consumer catch up.
            rw.readTestRecords(more);

            // Make sure lag goes back to zero and offset catches up.
            waitForMetric(metrics, "lag", 0);
            waitForMetric(metrics, "offset", n + more);
        }
    }

    private void waitForMetric(final OffsetMetrics metrics, final String nameSuffix, final long value)
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
