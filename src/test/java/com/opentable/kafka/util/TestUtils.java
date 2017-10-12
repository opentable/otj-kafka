package com.opentable.kafka.util;

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

import com.opentable.kafka.embedded.EmbeddedKafkaBroker;
import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;

public final class TestUtils {
    private TestUtils() {
    }

    private static final Duration LOOP_SLEEP = Duration.ofSeconds(1);
    static final String TOPIC_NAME = "topic-1";
    static final String GROUP_ID = "group-1";

    static EmbeddedKafkaBroker broker() throws InterruptedException {
        final EmbeddedKafkaBroker ekb = new EmbeddedKafkaBuilder().withTopics(TOPIC_NAME).start();
        waitForTopic(ekb);
        waitForCoordinator(ekb);
        return ekb;
    }

    static ProducerRecord<String, String> record(final int i) {
        return new ProducerRecord<>(
                TOPIC_NAME,
                String.format("key-%d", i),
                String.format("value-%d", i)
        );
    }

    static void loopSleep() throws InterruptedException {
        Thread.sleep(LOOP_SLEEP.toMillis());
    }

    static void writeTestRecords(final EmbeddedKafkaBroker ekb, final int lo, final int hi) {
        try (KafkaProducer<String, String> producer = ekb.createProducer()) {
            for (int i = lo; i <= hi; ++i) {
                producer.send(TestUtils.record(i));
            }
            producer.flush();
        }
    }

    static void readTestRecords(final EmbeddedKafkaBroker ekb, final int expect) {
        try (KafkaConsumer<String, String> consumer = ekb.createConsumer(GROUP_ID)) {
            consumer.subscribe(Collections.singleton(TestUtils.TOPIC_NAME));
            ConsumerRecords<String, String> records;
            while (true) {
                records = consumer.poll(Duration.ofSeconds(1).toMillis());
                if (!records.isEmpty()) {
                    break;
                }
            }
            Assertions.assertThat(records.count()).isEqualTo(expect);
            // Commit offsets.
            consumer.commitSync();
        }
    }

    private static void waitForTopic(final EmbeddedKafkaBroker ekb) throws InterruptedException {
        try (OffsetMonitor mon = new OffsetMonitor("test-topic-wait", ekb.getKafkaBrokerConnect())) {
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

    private static void waitForCoordinator(final EmbeddedKafkaBroker ekb) throws InterruptedException {
        try (OffsetMonitor mon = new OffsetMonitor("test-coord-wait", ekb.getKafkaBrokerConnect())) {
            while (true) {
                try {
                    mon.getGroupOffsets("no-group-1", TestUtils.TOPIC_NAME);
                } catch (final TimeoutException e) {
                    if (!(e.getCause() instanceof CoordinatorNotAvailableException)) {
                        throw e;
                    }
                    loopSleep();
                    continue;
                }
                break;
            }
        }
    }
}
