package com.opentable.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.omg.PortableInterceptor.NON_EXISTENT;

import com.opentable.kafka.embedded.EmbeddedKafkaBroker;
import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;
import com.opentable.kafka.util.OffsetMonitor;

public class OffsetMonitorTest {
    private static final String TOPIC_NAME = "topic-1";
    private static final String MISSING_TOPIC_NAME = "missing-topic-1";
    private static final String GROUP_ID = "group-1";
    private static final Duration LOOP_SLEEP = Duration.ofSeconds(1);

    // Would be nice to split this up into separate tests for easier reading.
    @Test(timeout = 60_000)
    public void test() throws InterruptedException {
        Map<Integer, Long> sizes, offsets;

        try (EmbeddedKafkaBroker ekb = broker()) {
            waitForTopic(ekb);
            try (OffsetMonitor monitor = new OffsetMonitor("test", ekb.getKafkaBrokerConnect());
                 KafkaProducer<String, String> producer = ekb.createProducer()) {
                // Test that we can get right answer for empty topic.
                sizes = monitor.getTopicSizes(TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                Assertions.assertThat(sizes).isNotEmpty();
                Assertions.assertThat(sizes).containsOnlyKeys(0);
                Assertions.assertThat(sizes.get(0)).isEqualTo(0L);

                // Test that missing topic is reflected as missing.
                sizes = monitor.getTopicSizes(MISSING_TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                Assertions.assertThat(sizes).isEmpty();

                // Wait for coordinator to become available.
                offsets = Collections.singletonMap(-1, -1L);
                while (true) {
                    try {
                        offsets = monitor.getGroupOffsets(GROUP_ID, TOPIC_NAME);
                    } catch (final TimeoutException e) {
                        if (!(e.getCause() instanceof CoordinatorNotAvailableException)) {
                            throw e;
                        }
                        loopSleep();
                        continue;
                    }
                    break;
                }

                // Test behavior for non-existent consumer group.
                Assertions.assertThat(offsets).isNotNull();
                Assertions.assertThat(offsets).isEmpty();

                final int numTestRecords = 3;

                // Produce some test data on the topic.
                for (int i = 1; i <= numTestRecords; ++i) {
                    producer.send(record(i));
                }
                producer.flush();

                try (KafkaConsumer<String, String> consumer = ekb.createConsumer(GROUP_ID)) {
                    // Read it.
                    consumer.subscribe(Collections.singleton(TOPIC_NAME));
                    ConsumerRecords<String, String> records;
                    while (true) {
                        records = consumer.poll(Duration.ofSeconds(1).toMillis());
                        if (!records.isEmpty()) {
                            break;
                        }
                    }
                    Assertions.assertThat(records.count()).isEqualTo(numTestRecords);
                    // Commit offsets.
                    consumer.commitSync();

                    // Make sure we can read them back with the monitor.
                    while (true) {
                        offsets = monitor.getGroupOffsets(GROUP_ID, TOPIC_NAME);
                        if (!offsets.isEmpty()) {
                            break;
                        }
                        loopSleep();
                    }
                    Assertions.assertThat(offsets).isNotNull();
                    Assertions.assertThat(offsets).isNotEmpty();
                    Assertions.assertThat(offsets).containsOnlyKeys(0);
                    Assertions.assertThat(offsets.get(0)).isEqualTo(numTestRecords);

                    // Ensure correct behavior for querying on existing consumer group, but for topic it's not
                    // consuming.
                    offsets = monitor.getGroupOffsets(GROUP_ID, MISSING_TOPIC_NAME);
                    Assertions.assertThat(offsets).isNotNull();
                    Assertions.assertThat(offsets).isEmpty();
                }

                // Ensure monitor reflects correct size of topic with data.
                sizes = monitor.getTopicSizes(TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                Assertions.assertThat(sizes).isNotEmpty();
                Assertions.assertThat(sizes).containsOnlyKeys(0);
                Assertions.assertThat(sizes.get(0)).isEqualTo(numTestRecords);
            }
        }
    }

    private EmbeddedKafkaBroker broker() {
        return new EmbeddedKafkaBuilder().withTopics(TOPIC_NAME).start();
    }

    private void waitForTopic(final EmbeddedKafkaBroker ekb) throws InterruptedException {
        try (OffsetMonitor mon = new OffsetMonitor("test-wait", ekb.getKafkaBrokerConnect())) {
            Map<Integer, Long> sizes;
            while (true) {
                sizes = mon.getTopicSizes(TOPIC_NAME);
                Assertions.assertThat(sizes).isNotNull();
                // Takes a little bit for the test topic to be created.
                if (!sizes.isEmpty()) {
                    break;
                }
                loopSleep();
            }
        }
    }

    private ProducerRecord<String, String> record(final int i) {
        return new ProducerRecord<>(
                TOPIC_NAME,
                String.format("key-%d", i),
                String.format("value-%d", i)
        );
    }

    private void loopSleep() throws InterruptedException {
        Thread.sleep(LOOP_SLEEP.toMillis());
    }
}
