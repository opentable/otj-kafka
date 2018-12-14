/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opentable.kafka.util;

import java.io.Closeable;
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

final class ReadWriteRule extends ExternalResource implements Closeable, AutoCloseable {
    private static final Duration POLL_TIME = Duration.ofSeconds(1);
    private static final String TOPIC_NAME = "topic-1";
    private static final String GROUP_ID = "group-1";

    private final EmbeddedKafkaBroker ekb;

    ReadWriteRule() {
        this(1);
    }

    ReadWriteRule(final int nPartitions) {
        ekb = new EmbeddedKafkaBuilder()
                .withTopics(TOPIC_NAME)
                .nPartitions(nPartitions)
                .start();
    }

    // No before @Override; ekb is initialized at object construction above.

    @Override
    protected void after() {
        close();
    }

    @Override
    public void close() {
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

    // Contains infinite loop.  Make sure to include a timeout for the test using this.
    void readTestRecords(final int expect) {
        try (KafkaConsumer<String, String> consumer = ekb.createConsumer(GROUP_ID)) {
            consumer.subscribe(Collections.singleton(ReadWriteRule.TOPIC_NAME));
            ConsumerRecords<String, String> records;
            while (true) {
                records = consumer.poll(POLL_TIME);
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
        Thread.sleep(POLL_TIME.toMillis());
    }
}
