package com.opentable.kafka.util;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.junit.rules.ExternalResource;

import com.opentable.kafka.embedded.EmbeddedKafkaBroker;
import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;

final class ReadWriteRule extends ExternalResource {
    private static final Duration LOOP_SLEEP = Duration.ofSeconds(1);
    private static final String TOPIC_NAME = "topic-1";
    private static final String GROUP_ID = "group-1";

    private final EmbeddedKafkaBroker ekb = new EmbeddedKafkaBuilder().withTopics(TOPIC_NAME).start();

    // No before @Override; ekb is initialized at object construction above.

    @Override
    protected void after() {
        ekb.close();
    }

    EmbeddedKafkaBroker getBroker() {
        return ekb;
    }

    String getTopicName() {
        return TOPIC_NAME;
    }

    String getGroupId() {
        return GROUP_ID;
    }

    ProducerRecord<String, String> record(final int i) {
        return new ProducerRecord<>(
                TOPIC_NAME,
                String.format("key-%d", i),
                String.format("value-%d", i)
        );
    }

    void writeTestRecords(final int lo, final int hi) {
        try (KafkaProducer<String, String> producer = ekb.createProducer()) {
            for (int i = lo; i <= hi; ++i) {
                producer.send(record(i));
            }
            producer.flush();
        }
    }

    void readTestRecords(final int expect) {
        try (KafkaConsumer<String, String> consumer = ekb.createConsumer(GROUP_ID)) {
            consumer.subscribe(Collections.singleton(ReadWriteRule.TOPIC_NAME));
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

    static void loopSleep() throws InterruptedException {
        Thread.sleep(LOOP_SLEEP.toMillis());
    }
}