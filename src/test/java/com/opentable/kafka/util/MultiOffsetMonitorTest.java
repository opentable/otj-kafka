package com.opentable.kafka.util;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;
import com.opentable.kafka.embedded.EmbeddedKafkaRule;

public class MultiOffsetMonitorTest {
    private static final Logger LOG = LoggerFactory.getLogger(MultiOffsetMonitorTest.class);

    private static final String TOPIC_NAME = "test-topic";
    /** Odd number so they <em>can't</em> be divided evenly between two monitors. */
    private static final int N_PARTITIONS = 9;
    private static final String GROUP_ID = "test-group";
    private static final String NAMESPACE = "test";

    @Rule
    public final EmbeddedKafkaRule ekr = new EmbeddedKafkaBuilder()
            .withTopics(TOPIC_NAME)
            .nPartitions(N_PARTITIONS)
            .rule();

    /** Test that multiple monitors with the same namespace and attached to the same broker, get the same view. */
    @Test(timeout = 60_000)
    public void test() throws InterruptedException, ExecutionException {
        final String brokerList = ekr.getBroker().getKafkaBrokerConnect();

        // Make consumer that will read from broker.
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, (int) Duration.ofSeconds(1).toMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(60).toMillis());
        final Deserializer<byte[]> deser = Serdes.ByteArray().deserializer();

        final Serializer<byte[]> ser = Serdes.ByteArray().serializer();

        try (
                KafkaProducer<byte[], byte[]> producer = ekr.getBroker().createProducer(ser, ser);
                KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props, deser, deser);
                OffsetMonitor mon1 = new OffsetMonitor(NAMESPACE, brokerList);
                OffsetMonitor mon2 = new OffsetMonitor(NAMESPACE, brokerList);
        ) {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 10_000; ++i) {
                final byte[] key = RandomStringUtils.randomAlphanumeric(20).getBytes(StandardCharsets.UTF_8);
                final byte[] value = RandomStringUtils.randomAlphanumeric(20).getBytes(StandardCharsets.UTF_8);
                futures.add(producer.send(new ProducerRecord<>(TOPIC_NAME, key, value)));
            }
            for (final Future future : futures) {
                future.get();
            }

            Map<Integer, Long> mon1TopicSizes;
            Map<Integer, Long> mon2TopicSizes;
            Map<Integer, Long> mon1GroupOffsets;
            Map<Integer, Long> mon2GroupOffsets;

            // Wait for offset record to fill.
            consumer.subscribe(Collections.singleton(TOPIC_NAME));
            while (true) {
                LOG.info("consumed {} records", consumer.poll(Duration.ofSeconds(1).toMillis()).count());

                mon1TopicSizes = mon1.getTopicSizes(TOPIC_NAME);
                mon2TopicSizes = mon2.getTopicSizes(TOPIC_NAME);
                mon1GroupOffsets = mon1.getGroupOffsets(GROUP_ID, TOPIC_NAME);
                mon2GroupOffsets = mon2.getGroupOffsets(GROUP_ID, TOPIC_NAME);
                LOG.info("key set sizes {} {} {} {}",
                        mon1TopicSizes.keySet().size(),
                        mon2TopicSizes.keySet().size(),
                        mon1GroupOffsets.keySet().size(),
                        mon2GroupOffsets.keySet().size());

                if (mon1TopicSizes.keySet().size() == N_PARTITIONS &&
                        mon2TopicSizes.keySet().size() == N_PARTITIONS &&
                        mon1GroupOffsets.keySet().size() == N_PARTITIONS &&
                        mon2GroupOffsets.keySet().size() == N_PARTITIONS) {
                    break;
                }

                Thread.sleep(Duration.ofSeconds(1).toMillis());
            }

            Assertions.assertThat(mon1TopicSizes).isEqualTo(mon2TopicSizes);
            Assertions.assertThat(mon1GroupOffsets).isEqualTo(mon2GroupOffsets);

            Assertions.assertThat(mon1TopicSizes.keySet()).isEqualTo(mon1GroupOffsets.keySet());
            Assertions.assertThat(mon2TopicSizes.keySet()).isEqualTo(mon2GroupOffsets.keySet());
        }
    }
}
