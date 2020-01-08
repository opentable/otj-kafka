package com.opentable.kafka.session;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.Test;

import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;
import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.builders.KafkaProducerBuilder;
import com.opentable.kafka.builders.SettableEnvironmentProvider;

public class TestLargeItems {

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
        String topic = "OT.MDA.Test.TestTopic";
        Producer<Integer,byte[]> producer =  producer();
        byte[] recordValue = getBigString(1024);
        RecordMetadata recordMetadata = producer().send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());
        recordValue = getBigString(1024 * 1023);
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

        // All others fail
        recordValue = getBigString(1024 * 1024);
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

        recordValue = getBigString(1024 * 1024 * 2);
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

        recordValue = getBigString(1024 * 1024 * 4);
        recordMetadata = producer.send(new ProducerRecord<>(topic, recordValue)).get();
        System.err.println("produced to " + recordMetadata.offset() + " " + recordMetadata.partition());

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

    protected Producer<Integer, byte[]> producer() {
        return producer("eda-kafka-feeder-ci-sf-01.qasql.opentable.com:9092");
    }

    protected Consumer<Integer, byte[]> consumer() {
        return consumer(1, "mygroup","eda-kafka-feeder-ci-sf-01.qasql.opentable.com:9092");
    }
    protected Producer<Integer, byte[]> producer(String bootstrapServers) {
        return producerBuilder()
                .withSerializers(IntegerSerializer.class, ByteArraySerializer.class)
                .withBootstrapServer(bootstrapServers)
                .withClientId(UUID.randomUUID().toString()) // each producer is unique
                .disableLogging()
                .disableMetrics()
                .build();
    }

    protected Consumer<Integer, byte[]> consumer(int consumerNumber, String groupId, String bootstrapServers) {
        return consumerBuilder()
                .withDeserializers(IntegerDeserializer.class, ByteArrayDeserializer.class)
                .withGroupId(groupId) // force new offset management
                .withBootstrapServer(bootstrapServers)
                .withAutoCommit(true)
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                .withClientId("consumer-" + consumerNumber) // each is tied to the consumer number
                .withMaxPollRecords(1) // 1 each time we poll makes book keeping much easier.
                // Seems to take in the output log, and should cause rebalance shortly after 10s
                .withSessionTimeoutMs(Duration.ofSeconds(10))
                .withProperty("max.poll.interval.ms", (int) Duration.ofSeconds(11).toMillis())
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
