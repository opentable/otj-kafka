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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.rules.ExternalResource;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;

public class KafkaBrokerRule extends ExternalResource
{
    private final Supplier<String> zookeeperConnectString;
    private KafkaServer kafka;
    private int port;

    private final List<String> topicsToCreate = new ArrayList<>();

    public KafkaBrokerRule(ZookeeperRule zk)
    {
        this(zk::getConnectString);
    }

    public KafkaBrokerRule(Supplier<String> zookeeperConnectString)
    {
        this.zookeeperConnectString = zookeeperConnectString;
    }

    public KafkaBrokerRule withTopics(String... topics) {
        topicsToCreate.addAll(Arrays.asList(topics));
        return this;
    }

    @Override
    protected void before() throws Throwable
    {
        kafka = new KafkaServer(createConfig(),
                KafkaServer.$lessinit$greater$default$2(), KafkaServer.$lessinit$greater$default$3());
        kafka.startup();

        topicsToCreate.forEach(this::createTopic);
    }

    @Override
    protected void after()
    {
        kafka.shutdown();
    }

    private KafkaConfig createConfig()
    {
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Properties config = TestUtils.createBrokerConfig(1, zookeeperConnectString.get(),
                TestUtils.createBrokerConfig$default$3(), TestUtils.createBrokerConfig$default$4(),
                port, TestUtils.createBrokerConfig$default$6(),
                TestUtils.createBrokerConfig$default$7(), TestUtils.createBrokerConfig$default$8(),
                TestUtils.createBrokerConfig$default$9(), TestUtils.createBrokerConfig$default$10(),
                TestUtils.createBrokerConfig$default$11(), TestUtils.createBrokerConfig$default$12(),
                TestUtils.createBrokerConfig$default$13(), TestUtils.createBrokerConfig$default$14());
        return new KafkaConfig(config);
    }

    public String getKafkaBrokerConnect()
    {
        Preconditions.checkState(port > 0, "no port set yet");
        return "localhost:" + port;
    }

    public KafkaServer getServer()
    {
        return kafka;
    }

    public void createTopic(String topic) {
        AdminUtils.createTopic(kafka.zkUtils(), topic, 1, 1, new Properties());
    }

    public KafkaConsumer<String, String> createConsumer(String groupId) {
        return createConsumer(groupId, StringDeserializer.class);
    }

    public <V> KafkaConsumer<String, V> createConsumer(String groupId, Class<? extends Deserializer<V>> valueDeser) {
        return createConsumer(groupId, StringDeserializer.class, valueDeser);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(String groupId, Class<? extends Deserializer<K>> keyDeser, Class<? extends Deserializer<V>> valueDeser) {
        Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        props.put("bootstrap.servers", getKafkaBrokerConnect());
        props.put("group.id", groupId);
        props.put("key.deserializer", keyDeser.getName());
        props.put("value.deserializer", valueDeser.getName());
        return new KafkaConsumer<>(props);
    }

    public KafkaProducer<String, String> createProducer() {
        return createProducer(StringSerializer.class);
    }

    public <V> KafkaProducer<String, V> createProducer(Class<? extends Serializer<V>> valueSer) {
        return createProducer(StringSerializer.class, valueSer);
    }

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", "3");
        props.put("bootstrap.servers", getKafkaBrokerConnect());
        props.put("key.serializer", keySer.getName());
        props.put("value.serializer", valueSer.getName());
        return new KafkaProducer<>(props);
    }
}
