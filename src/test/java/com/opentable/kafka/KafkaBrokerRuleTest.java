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
package com.opentable.kafka;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import com.opentable.kafka.embedded.EmbeddedKafkaBroker;
import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;
import com.opentable.kafka.embedded.EmbeddedKafkaRule;

public class KafkaBrokerRuleTest {
    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_VALUE = "The quick brown fox jumps over the lazy dog.";

    @Rule
    public final EmbeddedKafkaRule kb = new EmbeddedKafkaBuilder()
            .withTopics(TEST_TOPIC)
            .rule();

    @Test(timeout = 30000)
    public void testKafkaRule() throws Exception {
        EmbeddedKafkaBroker ekb = kb.getBroker();

        try (KafkaProducer<String, String> producer = ekb.createProducer()) {
            producer.send(new ProducerRecord<String, String>(TEST_TOPIC, TEST_VALUE));
        }

        try (KafkaConsumer<String, String> consumer = ekb.createConsumer("test")) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            assertEquals(1, records.count());
            assertEquals(TEST_VALUE, records.iterator().next().value());
        }
    }
}
