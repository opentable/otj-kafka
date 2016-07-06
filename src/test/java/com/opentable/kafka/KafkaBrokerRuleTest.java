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

import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class KafkaBrokerRuleTest {
    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_VALUE = "The quick brown fox jumps over the lazy dog.";

    private final ZookeeperRule zk = new ZookeeperRule();
    private final KafkaBrokerRule kb = new KafkaBrokerRule(zk);

    @Rule
    public RuleChain rules = RuleChain.outerRule(zk).around(kb);

    @Test(timeout = 30000)
    public void testKafkaRule() throws Exception {
        kb.createTopic(TEST_TOPIC);

        try (KafkaProducer<String, String> producer = kb.createProducer()) {
            producer.send(new ProducerRecord<String, String>(TEST_TOPIC, TEST_VALUE));
        }

        try (KafkaConsumer<String, String> consumer = kb.createConsumer("test")) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(5000);
            assertEquals(1, records.count());
            assertEquals(TEST_VALUE, records.iterator().next().value());
        }
    }
}
