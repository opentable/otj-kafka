package com.opentable.kafka.util;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.opentable.kafka.embedded.EmbeddedKafkaBroker;

public class OffsetMonitorTest {
    private static final String MISSING_TOPIC_NAME = "missing-topic-1";

    // Would be nice to split this up into separate tests for easier reading.
    @Test(timeout = 60_000)
    public void test() throws InterruptedException {
        Map<Integer, Long> sizes, offsets;

        try (EmbeddedKafkaBroker ekb = TestUtils.broker()) {
            try (OffsetMonitor monitor = new OffsetMonitor("test", ekb.getKafkaBrokerConnect());
                 KafkaProducer<String, String> producer = ekb.createProducer()) {
                // Test that we can get right answer for empty topic.
                sizes = monitor.getTopicSizes(TestUtils.TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                Assertions.assertThat(sizes).isNotEmpty();
                Assertions.assertThat(sizes).containsOnlyKeys(0);
                Assertions.assertThat(sizes.get(0)).isEqualTo(0L);

                // Test that missing topic is reflected as missing.
                sizes = monitor.getTopicSizes(MISSING_TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                Assertions.assertThat(sizes).isEmpty();

                // Test behavior for non-existent consumer group.
                offsets = monitor.getGroupOffsets(TestUtils.GROUP_ID, TestUtils.TOPIC_NAME);
                Assertions.assertThat(offsets).isNotNull();
                Assertions.assertThat(offsets).isEmpty();

                final int numTestRecords = 3;
                TestUtils.writeTestRecords(ekb, 1, numTestRecords);

                TestUtils.readTestRecords(ekb, numTestRecords);
                // Make sure we can read them back with the monitor.
                while (true) {
                    offsets = monitor.getGroupOffsets(TestUtils.GROUP_ID, TestUtils.TOPIC_NAME);
                    if (!offsets.isEmpty()) {
                        break;
                    }
                    TestUtils.loopSleep();
                }
                Assertions.assertThat(offsets).isNotNull();
                Assertions.assertThat(offsets).isNotEmpty();
                Assertions.assertThat(offsets).containsOnlyKeys(0);
                Assertions.assertThat(offsets.get(0)).isEqualTo(numTestRecords);

                // Ensure correct behavior for querying on existing consumer group, but for topic it's not
                // consuming.
                offsets = monitor.getGroupOffsets(TestUtils.GROUP_ID, MISSING_TOPIC_NAME);
                Assertions.assertThat(offsets).isNotNull();
                Assertions.assertThat(offsets).isEmpty();

                // Ensure monitor reflects correct size of topic with data.
                sizes = monitor.getTopicSizes(TestUtils.TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                Assertions.assertThat(sizes).isNotEmpty();
                Assertions.assertThat(sizes).containsOnlyKeys(0);
                Assertions.assertThat(sizes.get(0)).isEqualTo(numTestRecords);
            }
        }
    }
}
