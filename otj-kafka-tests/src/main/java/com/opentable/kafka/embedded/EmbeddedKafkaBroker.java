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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.serialization.Deserializer;
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
    private static final Duration GLOBAL_OPERATION_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration LOOP_SLEEP = Duration.ofMillis(500);

    private final List<String> topicsToCreate;

    private EmbeddedZookeeper ezk;
    private KafkaServer kafka;
    private AdminClient admin;
    private final boolean autoCreateTopics;
    private final int nPartitions;

    private Path stateDir;

    public EmbeddedKafkaBroker(
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

        try {
            LOG.info("waiting for embedded kafka broker to become ready");
            admin = AdminClient.create(createAdminProperties());
            waitForCoordinator();

            LOG.info("About to create topics");
            topicsToCreate.forEach(this::createTopic);
            waitForTopics();
            maybeCreateConsumerOffsets();
            LOG.info("EmbeddedKafkaBroker start complete");

        } catch (final InterruptedException e) {
            LOG.error("interrupted waiting for broker to become ready", e);
            Thread.currentThread().interrupt();
        }

        return this;
    }

    // Create consumer offsets ourselves so we can control replication, partitions, etc
    private void maybeCreateConsumerOffsets() throws InterruptedException {
        final String consumerOffsets = "__consumer_offsets";
        retry("create consumer offsets", () -> {
            try {
                Map<String, TopicDescription> description = admin.describeTopics(Collections.singleton(consumerOffsets)).all().get(10, TimeUnit.SECONDS);
                LOG.info("topic {} already exists, size {}", consumerOffsets, description.size());
            } catch (ExecutionException executionException) {
                if (Throwables.getRootCause(executionException) instanceof UnknownTopicOrPartitionException) {
                    LOG.info("topic not found, will try to create topic {}", consumerOffsets);
                    createTopic(consumerOffsets);
                    return;
                }
                // if we get here, then it's not an exception we know how to handle, thus rethrow
                throw executionException;
            }
        });
    }

    @Override
    @PreDestroy
    @SuppressWarnings("PMD.UseTryWithResources")
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
            throw new RuntimeException(e);
        }
    }

    private void waitForTopics() throws InterruptedException {
        final Instant start = Instant.now();
        LOG.info("waiting for topics");
        retry("topics available", () -> {
            Map<String, TopicDescription> res = admin.describeTopics(topicsToCreate).all().get();
            for (String t : topicsToCreate) {
                List<TopicPartitionInfo> partitions = Optional.ofNullable(res.get(t))
                    .map(TopicDescription::partitions)
                    .orElse(Collections.emptyList());
                if (partitions.size() < nPartitions) {
                    throw new KafkaException("partitions not ready yet (" + nPartitions + "): " + partitions);
                }
            }
        });
        LOG.info("topics ready, all having {} partition{}, took{}",
                nPartitions, nPartitions != 1 ? "s" : "", Duration.between(start, Instant.now()));
    }

    private void waitForCoordinator() throws InterruptedException {
        retry("coordinator available", () -> admin.describeCluster().controller().get(10, TimeUnit.SECONDS));
    }

    private static void retry(String description, RetryAction action) {
        final Instant start = Instant.now();
        Exception last;
        while (true) {
            try {
                LOG.info("start {}", description);
                action.run();
                LOG.info("{} after {}", description, Duration.between(start, Instant.now()));
                return;
            } catch (KafkaException e) {
                LOG.debug("retrying due to {}", e.toString());
                last = e;
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof KafkaException)) {
                    throw new RuntimeException(e);
                }
                LOG.debug("retrying due to {}", e.toString());
                last = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted", e);
            } catch (TimeoutException e) {
                throw new RuntimeException("timeout", e);
            }
            loopSleep(start, last);
        }
    }

    private static void loopSleep(final Instant start, Exception last) {
        try {
            Thread.sleep(LOOP_SLEEP.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted", e);
        }
        if (Instant.now().isAfter(start.plus(GLOBAL_OPERATION_TIMEOUT))) {
            throw new RuntimeException("operation timed out", last);
        }
    }

    private KafkaConfig createConfig()
    {
        return new KafkaConfig(createProperties());
    }

    private Properties createProperties() {
        Properties config = new Properties();
        config.put(KafkaConfig.ZkConnectProp(), ezk.getConnectString());
        config.put(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:0");
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

    private Properties createAdminProperties() {
        Properties config = createProperties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerConnect());
        return config;
    }

    public String getKafkaBrokerConnect()
    {
        Preconditions.checkState(kafka !=  null, "broker not started yet");
        return "localhost:" + kafka.boundPort(ListenerName.normalised("PLAINTEXT"));
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

    public Properties baseConsumerProperties(String groupId) {
        Properties props = new Properties();
        Map<String, Object> map = baseConsumerMap(groupId);
        map.forEach(props::put);
        return props;
    }

    public Map<String, Object> baseConsumerMap(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put("auto.offset.reset", "earliest");
        props.put("bootstrap.servers", getKafkaBrokerConnect());
        props.put("group.id", groupId);
        return props;
    }

    public Properties baseProducerProperties() {
        Properties props = new Properties();
        Map<String, Object> map = baseProducerMap();
        map.forEach(props::put);
        return props;
    }

    public Map<String, Object> baseProducerMap() {
        Map<String, Object> props = new HashMap<>();
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("bootstrap.servers", getKafkaBrokerConnect());
        return props;
    }

    interface RetryAction {
        void run() throws ExecutionException, InterruptedException, KafkaException, TimeoutException;
    }
}
