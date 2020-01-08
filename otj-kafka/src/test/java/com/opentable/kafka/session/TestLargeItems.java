package com.opentable.kafka.session;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;
import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.builders.KafkaProducerBuilder;
import com.opentable.kafka.builders.SettableEnvironmentProvider;

public class TestLargeItems {
    String topic = "OT.MDA.Test.TestTopic";
    @Test(timeout = 10000)
    public void testPublishBigItem() throws ExecutionException, InterruptedException {
        /*
        produced to 588 10
2020-01-08T01:14:45.878Z  INFO <> --- [kafka-producer-network-thread | 064e0707-efb5-4eba-8ef3-b4f0480d788b] org.apache.kafka.clients.Metadata        : Cluster ID: qxt8oLeyRR-d3tuHlct82g
produced to 564 16

java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1048664 bytes when serialized which is larger than the maximum request size you have configured with the max.request.size configuration.

	at org.apache.kafka.clients.producer.KafkaProducer$FutureFailure.<init>(KafkaProducer.java:1186)
	at org.apache.kafka.clients.producer.KafkaProducer.doSend(KafkaProducer.java:880)
	at org.apache.kafka.clients.producer.KafkaProducer.send(KafkaProducer.java:803)
	at org.apache.kafka.clients.producer.KafkaProducer.send(KafkaProducer.java:690)
	at com.opentable.kafka.session.TestLargeItems.testPublishBigItem(TestLargeItems.java:42)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
	at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
	at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
	at org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)
	at org.junit.internal.runners.statements.FailOnTimeout$CallableStatement.call(FailOnTimeout.java:298)
	at org.junit.internal.runners.statements.FailOnTimeout$CallableStatement.call(FailOnTimeout.java:292)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.lang.Thread.run(Thread.java:748)
Caused by: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1048664 bytes when serialized which is larger than the maximum request size you have configured with the max.request.size configuration.

         */

        Producer<byte[],byte[]> producer =  producer();
        byte[] recordValue = getBigString(1024);
        System.err.println("Checksum: " + DigestUtils.md5Hex(recordValue));
        RecordMetadata recordMetadata = producer().send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());
        recordValue = getBigString(1024 * 1023);
        System.err.println("Checksum: " + DigestUtils.md5Hex(recordValue));
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

        // All others fail
        recordValue = getBigString(1024 * 1024);
        System.err.println("Checksum: " + DigestUtils.md5Hex(recordValue));
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

        recordValue = getBigString(1024 * 1024 * 2);
        System.err.println("Checksum: " + DigestUtils.md5Hex(recordValue));
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

        recordValue = getBigString(1024 * 1024 * 4);
        System.err.println("Checksum: " + DigestUtils.md5Hex(recordValue));
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

    }

    // This test will always timeout
    @Test(timeout = 30000)
    public void consumeMessages() {
        Consumer<byte[], byte[]> consumer = consumer("mygroup3");
        consumer.subscribe(Collections.singletonList(topic));
        while(true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                System.err.println("Received " + records.count());
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] v = record.value();
                    long o = record.offset();
                    int p = record.partition();
                    System.err.println("Checksum: " + DigestUtils.md5Hex(v));
                    System.err.println("RECEIVED p=" + p + ", o="+ o+", v=(size=" + v.length +" )");
                }
            }
        }
    }

    private byte[] getBigString(int msgSize) {
        byte[] bytes = new byte[msgSize];
        byte current = (byte) 0;
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = current;
            if (current < ((byte) 255)) {
                current++;
            } else {
                current = 0;
            }
        }
        return bytes;
    }

    protected Producer<byte[], byte[]> producer() {
        return producer("eda-kafka-feeder-ci-sf-01.qasql.opentable.com:9092");
    }

    protected Consumer<byte[], byte[]> consumer(String groupId ) {
        return consumer(1, groupId,"eda-kafka-feeder-ci-sf-01.qasql.opentable.com:9092");
    }
    protected Producer<byte[], byte[]> producer(String bootstrapServers) {
           return producerBuilder().withBootstrapServer(bootstrapServers)
                .withBatchSize(100)
                .withRetries(3)
                .withAcks(KafkaProducerBuilder.AckType.atleastOne)
                .withProperty("compression.type","snappy")
                .withSerializers(new ByteArraySerializer(), new ByteArraySerializer())
                .withClientId(UUID.randomUUID().toString()) // each producer is unique
                .disableLogging()
                .disableMetrics()
                .build();
    }

    protected Consumer<byte[], byte[]> consumer(int consumerNumber, String groupId, String bootstrapServers) {
        return consumerBuilder()
                .withDeserializers(ByteArrayDeserializer.class, ByteArrayDeserializer.class)
                .withGroupId(groupId) // force new offset management
                .withBootstrapServer(bootstrapServers)
                .withAutoCommit(true)
                .withProperty("compression.type","snappy")
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                .withClientId("consumer-" + consumerNumber) // each is tied to the consumer number
                .withMaxPollRecords(1) // 1 each time we poll makes book keeping much easier.
                .withAutoOffsetReset(KafkaConsumerBaseBuilder.AutoOffsetResetType.Earliest)
                .disableLogging()
                .disableMetrics()
                .build();
    }

    public KafkaConsumerBuilder<Integer, String> consumerBuilder() {
        return new KafkaConsumerBuilder<>(new HashMap<>(), new SettableEnvironmentProvider("","",1,"",""));
    }

    public KafkaProducerBuilder<Integer, String> producerBuilder() {
        return new KafkaProducerBuilder<>(new HashMap<>(), new SettableEnvironmentProvider("", "", 1, "", ""));
    }
}
