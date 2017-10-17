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
package com.opentable.kafka.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.base.Preconditions;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import com.opentable.io.DeleteRecursively;

public class EmbeddedKafkaBroker implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedKafkaBroker.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration LOOP_SLEEP = Duration.ofMillis(500);

    private final List<String> topicsToCreate;

    private EmbeddedZookeeper ezk;
    private KafkaServer kafka;
    private int port;
    private final boolean autoCreateTopics;

    private Path stateDir;

    protected EmbeddedKafkaBroker(final List<String> topicsToCreate, final boolean autoCreateTopics)
    {
        this.topicsToCreate = topicsToCreate;
        this.autoCreateTopics = autoCreateTopics;
    }

    @PostConstruct
    public EmbeddedKafkaBroker start()
    {
        try {
            stateDir = Files.createTempDirectory("embedded-kafka");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ezk = new EmbeddedZookeeper();
        ezk.start();
        kafka = new KafkaServer(createConfig(),
                KafkaServer.$lessinit$greater$default$2(),
                KafkaServer.$lessinit$greater$default$3(),
                KafkaServer.$lessinit$greater$default$4());
        LOG.info("Server created");
        kafka.startup();
        LOG.info("Server started up");

        topicsToCreate.forEach(this::createTopic);

        LOG.info("waiting for embedded kafka broker to become ready");
        try {
            waitUntilReady();
        } catch (final InterruptedException e) {
            LOG.error("interrupted waiting for broker to become ready", e);
            Thread.currentThread().interrupt();
        }

        return this;
    }

    @Override
    @PreDestroy
    public void close()
    {
        try {
            kafka.shutdown();
        } finally {
            ezk.close();
        }
        try {
            Files.walkFileTree(stateDir, DeleteRecursively.INSTANCE);
        } catch (IOException e) {
            LOG.error("while deleting {}", stateDir, e);
        }
    }

    private void waitUntilReady() throws InterruptedException {
        final Instant start = Instant.now();
        waitForTopics(start);
        waitForCoordinator(start);
    }

    private void waitForTopics(final Instant start) throws InterruptedException {
        LOG.info("waiting for topics");
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerConnect());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-wait-for-topics");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
        final Deserializer<byte[]> deser = Serdes.ByteArray().deserializer();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props, deser, deser)) {
            for (final String topic : topicsToCreate) {
                while (true) {
                    final List<PartitionInfo> parts = consumer.partitionsFor(topic);
                    if (parts != null) {
                        break;
                    }
                    loopSleep(start);
                }
            }
        }
    }

    private void waitForCoordinator(final Instant start) throws InterruptedException {
        LOG.info("waiting for coordinator");
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerConnect());
        final AdminClient adminClient = AdminClient.create(props);
        try {
            while (true) {
                try {
                    adminClient.listGroupOffsets("ot-kafka-embedded-not-a-real-group");
                    break;
                } catch (final TimeoutException e) {
                    if (!(e.getCause() instanceof CoordinatorNotAvailableException)) {
                        throw e;
                    }
                    loopSleep(start);
                }
            }
        } finally {
            adminClient.close();
        }
    }

    private static void loopSleep(final Instant start) throws InterruptedException {
        Thread.sleep(LOOP_SLEEP.toMillis());
        if (Instant.now().isAfter(start.plus(TIMEOUT))) {
            throw new RuntimeException("timed out");
        }
    }

    private KafkaConfig createConfig()
    {
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Properties config = new Properties();
        config.put(KafkaConfig.ZkConnectProp(), ezk.getConnectString());
        config.put(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:" + port);
        config.put(KafkaConfig.LogDirProp(), stateDir.resolve("logs").toString());
        config.put(KafkaConfig.ZkConnectionTimeoutMsProp(), "10000");
        config.put(KafkaConfig.ReplicaSocketTimeoutMsProp(), "1500");
        config.put(KafkaConfig.ControllerSocketTimeoutMsProp(), "1500");
        config.put(KafkaConfig.ControlledShutdownEnableProp(), "true");
        config.put(KafkaConfig.DeleteTopicEnableProp(), "false");
        config.put(KafkaConfig.ControlledShutdownRetryBackoffMsProp(), "100");
        config.put(KafkaConfig.LogCleanerDedupeBufferSizeProp(), "2097152");
        config.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), (short) 1);
        config.put(KafkaConfig.AutoCreateTopicsEnableProp(), autoCreateTopics);
        return new KafkaConfig(config);
    }

    public String getKafkaBrokerConnect()
    {
        Preconditions.checkState(port > 0, "no port set yet");
        return "localhost:" + port;
    }

    public EmbeddedZookeeper getZookeeper() {
        return ezk;
    }

    public KafkaServer getServer()
    {
        return kafka;
    }

    public void createTopic(String topic) {
        AdminUtils.createTopic(kafka.zkUtils(), topic, 1, 1, new Properties(), new RackAwareMode.Safe$());
        LOG.info("Topic {} created", topic);
    }

    public KafkaConsumer<String, String> createConsumer(String groupId) {
        return createConsumer(groupId, StringDeserializer.class);
    }

    public <V> KafkaConsumer<String, V> createConsumer(String groupId, Class<? extends Deserializer<V>> valueDeser) {
        return createConsumer(groupId, StringDeserializer.class, valueDeser);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId, Class<? extends Deserializer<K>> keyDeser, Class<? extends Deserializer<V>> valueDeser) {
        Properties props = baseConsumerProperties(groupId);
        props.put("key.deserializer", keyDeser.getName());
        props.put("value.deserializer", valueDeser.getName());
        return new KafkaConsumer<>(props);
    }

    public <V> KafkaConsumer<String, V> createConsumer(String groupId, Deserializer<V> valueDeserializer) {
        return createConsumer(groupId, new StringDeserializer(), valueDeserializer);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<>(baseConsumerProperties(groupId), keyDeserializer, valueDeserializer);
    }

    public KafkaProducer<String, String> createProducer() {
        return createProducer(StringSerializer.class);
    }

    public <V> KafkaProducer<String, V> createProducer(Class<? extends Serializer<V>> valueSer) {
        return createProducer(StringSerializer.class, valueSer);
    }

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        Properties props = baseProducerProperties();
        props.put("key.serializer", keySer.getName());
        props.put("value.serializer", valueSer.getName());
        return new KafkaProducer<>(props);
    }

    public <V> KafkaProducer<String, V> createProducer(Serializer<V> valueSerializer) {
        return createProducer(new StringSerializer(), valueSerializer);
    }

    public <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new KafkaProducer<>(baseProducerProperties(), keySerializer, valueSerializer);
    }

    private Properties baseConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        props.put("bootstrap.servers", getKafkaBrokerConnect());
        props.put("group.id", groupId);
        return props;
    }

    private Properties baseProducerProperties() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("bootstrap.servers", getKafkaBrokerConnect());
        return props;
    }
}
