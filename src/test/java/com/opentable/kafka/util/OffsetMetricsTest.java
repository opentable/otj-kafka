package com.opentable.kafka.util;

import java.time.Duration;
import java.util.Collections;

import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

import com.opentable.kafka.embedded.EmbeddedKafkaBroker;

public class OffsetMetricsTest {
    private static final String METRIC_NS = "foobar";
    private static final String GROUP_ID = "group-1";

    @Test(timeout = 30_000, expected = IllegalArgumentException.class)
    public void testNoTopics() throws InterruptedException {
        try (EmbeddedKafkaBroker ekb = TestUtils.broker()) {
            new OffsetMetrics(METRIC_NS, new MetricRegistry(),
                    GROUP_ID, ekb.getKafkaBrokerConnect(), Collections.emptySet());
        }
    }

    @Test(timeout = 30_000, expected = IllegalArgumentException.class)
    public void testMissingTopic() throws InterruptedException {
        try (EmbeddedKafkaBroker ekb = TestUtils.broker()) {
            new OffsetMetrics(METRIC_NS, new MetricRegistry(),
                    GROUP_ID, ekb.getKafkaBrokerConnect(), Collections.singleton("no-topic-1"));
        }
    }

    @Test(timeout = 60_000)
    public void testMetrics() throws InterruptedException {
        try (EmbeddedKafkaBroker ekb = TestUtils.broker();
             KafkaProducer<String, String> producer = ekb.createProducer();
             OffsetMetrics metrics = new OffsetMetrics(
                     METRIC_NS,
                     new MetricRegistry(),
                     GROUP_ID,
                     ekb.getKafkaBrokerConnect(),
                     Collections.singleton(TestUtils.TOPIC_NAME),
                     Duration.ofMillis(500)
             )
        ) {
            metrics.start();

            // Write and read some test records.
            final int n = 10;
            TestUtils.writeTestRecords(ekb, 1, n);
            TestUtils.readTestRecords(ekb, n);

            // Ensure metrics true with records written and read.
            waitForMetric(metrics, "size", n);
            waitForMetric(metrics, "offset", n);
            waitForMetric(metrics, "lag", 0);

            // Write some more, but don't read yet.
            final int more = 3;
            TestUtils.writeTestRecords(ekb, n + 1, n + more);

            // Ensure metrics true up, including lag.
            waitForMetric(metrics, "size", n + more);
            waitForMetric(metrics, "lag", more);
            waitForMetric(metrics, "offset", n);

            // Have consumer catch up.
            TestUtils.readTestRecords(ekb, more);

            // Make sure lag goes back to zero and offset catches up.
            waitForMetric(metrics, "lag", 0);
            waitForMetric(metrics, "offset", n + more);
        }
    }

    private void waitForMetric(final OffsetMetrics metrics, final String nameSuffix, final long value)
            throws InterruptedException {
        final String metricName = String.format("%s.%s.partition.0.%s", METRIC_NS, TestUtils.TOPIC_NAME, nameSuffix);
        while (true) {
            final Counting c = (Counting) metrics.getMetrics().get(metricName);
            if (c.getCount() == value) {
                break;
            }
            TestUtils.loopSleep();
        }
    }
}
