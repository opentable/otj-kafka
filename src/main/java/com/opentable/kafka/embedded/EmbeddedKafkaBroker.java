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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.base.Preconditions;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private AdminClient admin;
    private int port;
    private final boolean autoCreateTopics;
    private final int nPartitions;

    private Path stateDir;

    protected EmbeddedKafkaBroker(
            final List<String> topicsToCreate,
            final boolean autoCreateTopics,
            final int nPartitions) {
        this.topicsToCreate = topicsToCreate;
        this.autoCreateTopics = autoCreateTopics;
        this.nPartitions = nPartitions;
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

        admin = AdminClient.create(createProperties());

        topicsToCreate.forEach(this::createTopic);

        LOG.info("waiting for embedded kafka broker to become ready");
        try {
            waitUntilReady();
            maybeCreateConsumerOffsets();
        } catch (final InterruptedException e) {
            LOG.error("interrupted waiting for broker to become ready", e);
            Thread.currentThread().interrupt();
        }

        return this;
    }

    // Create consumer offsets ourselves so we can control replication, partitions, etc
    private void maybeCreateConsumerOffsets() throws InterruptedException {
        final String consumerOffsets = "__consumer_offsets";
        Map<String, TopicDescription> description;
        final Instant start = Instant.now();
        LOG.info("Creating consumer offsets");
        while (true) {
            try {
                description = admin.describeTopics(Collections.singleton(consumerOffsets)).all().get(10, TimeUnit.SECONDS);
                if (description.isEmpty()) {
                    createTopic(consumerOffsets);
                }
                LOG.info("Consumer offsets ready after {}", Duration.between(start, Instant.now()));
                return;
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e1);
            } catch (ExecutionException | TimeoutException e1) {
                if (e1.getCause() instanceof LeaderNotAvailableException) {
                    loopSleep(start);
                    continue;
                }
                throw new RuntimeException(e1);
            }
        }
    }

    @Override
    @PreDestroy
    public void close()
    {
        try {
            admin.close();
        } finally {
            try {
                kafka.shutdown();
            } finally {
                ezk.close();
            }
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
                    try {
                        final List<PartitionInfo> parts = consumer.partitionsFor(topic);
                        if (parts != null && parts.size() == nPartitions) {
                            break;
                        }
                    } catch (KafkaException e) {
                        LOG.debug("Still waiting for topics: {}", e.toString());
                    }
                    loopSleep(start);
                }
            }
        }
        LOG.info("topics ready, all having {} partition{}, took{}",
                nPartitions, nPartitions != 1 ? "s" : "", Duration.between(start, Instant.now()));
    }

    private void waitForCoordinator(final Instant start) throws InterruptedException {
        while (true) {
            try {
                admin.describeCluster().controller().get(10, TimeUnit.SECONDS);
                break;
            } catch (final TimeoutException | ExecutionException e) {
                if (!(e.getCause() instanceof LeaderNotAvailableException)) {
                    throw new RuntimeException(e);
                }
                loopSleep(start);
            }
        }
        LOG.info("coordinator available after {}", Duration.between(start, Instant.now()));
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

        Properties config = createProperties();
        return new KafkaConfig(config);
    }

    private Properties createProperties() {
        Preconditions.checkState(port > 0, "no port set yet");

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerConnect());
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
        config.put(KafkaConfig.OffsetsTopicPartitionsProp(), 1);
        config.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), (short) 1);
        config.put(KafkaConfig.AutoCreateTopicsEnableProp(), autoCreateTopics);
        config.put(KafkaConfig.GroupMinSessionTimeoutMsProp(), 50);
        config.put(KafkaConfig.GroupInitialRebalanceDelayMsProp(), 50);
        return config;
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
        LOG.info("Creating topic {}", topic);
        admin.createTopics(Collections.singleton(new NewTopic(topic, nPartitions, (short) 1)));
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
