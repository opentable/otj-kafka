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

        try (KafkaConsumer<String, String> consumer = kb.createConsumer()) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(5000);
            assertEquals(1, records.count());
            assertEquals(TEST_VALUE, records.iterator().next().value());
        }
    }
}
