package com.opentable.kafka.logging;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import com.opentable.kafka.util.LogSamplerRandom;

public class LoggingKafkaProducer<K, V> implements Producer<K, V> {

    private final Producer<K, V> delegate;
    private String clientId;
    private LogSamplerRandom sampler = new LogSamplerRandom(5.0);

    public LoggingKafkaProducer(KafkaProducer<K, V> delegate) {
        this.delegate = delegate;
        borrowConfiguration();
    }

    @Override
    public void initTransactions() {
        delegate.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        delegate.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s)
        throws ProducerFencedException {
        delegate.sendOffsetsToTransaction(map, s);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        delegate.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        delegate.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    /*
    // Create wrappedRecord because headers can be read only in record (if record is sent second time)
    ProducerRecord<K, V> wrappedRecord = new ProducerRecord<>(record.topic(),
        record.partition(),
        record.timestamp(),
        record.key(),
        record.value(),
        record.headers());
    */
        LoggingUtils.setupHeaders(record);
        LoggingUtils.setupTracing(sampler, record);
        return delegate.send(record, new LoggingCallback<>(callback, record, clientId));
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return delegate.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        delegate.close(timeout, timeUnit);
    }


    private void borrowConfiguration() {
        try {
            clientId = (String) getField(delegate, "clientId");
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getField(Object obj, String name) throws IllegalAccessException, NoSuchFieldException {
        Class clazz = obj.getClass();
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(obj);
    }
}