package com.opentable.kafka.spring.builders;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaProducerBuilder;

public class KafkaProducerFactoryBuilder<K, V>  {

    private final KafkaProducerBuilder<K, V> delegate;

    public KafkaProducerFactoryBuilder(EnvironmentProvider environmentProvider) {
        this.delegate = new KafkaProducerBuilder<>(environmentProvider);
    }

    public KafkaProducerFactoryBuilder(Map<String, Object> prop, EnvironmentProvider environmentProvider) {
        this.delegate = new KafkaProducerBuilder<>(prop, environmentProvider);
    }

    public ProducerFactory<K, V> build() {
        return build(null, null);
    }

    public <K2, V2> ProducerFactory<K2, V2> build(@Nullable Serializer<K2> keySerializer, @Nullable Serializer<V2> valueSerializer) {
        return new DefaultKafkaProducerFactory<>(delegate.buildProps(), keySerializer, valueSerializer);
    }


    public KafkaProducerFactoryBuilder<K, V> withProperty(String key, Object value) {
        delegate.withProperty(key, value);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withProperties(Map<String, Object> config) {
        delegate.withProperties(config);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> removeProperty(String key) {
        delegate.removeProperty(key);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> disableLogging() {
        delegate.disableLogging();
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> disableMetrics() {
        delegate.disableMetrics();
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withSamplingRatePer10Seconds(int rate) {
        delegate.withSamplingRatePer10Seconds(rate);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withRandomSamplingRate(int rate) {
        delegate.withRandomSamplingRate(rate);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        delegate.withInterceptor(clazz);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withAcks(KafkaProducerBuilder.AckType val) {
        delegate.withAcks(val);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withRetries(int val) {
        delegate.withRetries(val);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withMaxInFlightRequests(int val) {
        delegate.withMaxInFlightRequests(val);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withBatchSize(int val) {
        delegate.withBatchSize(val);
        return this;
    }

    public <K2, V2> KafkaProducerFactoryBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        delegate.withSerializers(keySer, valSer);
        return (KafkaProducerFactoryBuilder<K2, V2>)this;
    }

    public <K2, V2> KafkaProducerFactoryBuilder<K2, V2> withSerializers(Serializer<K2> keySer, Serializer<V2> valSer) {
        delegate.withSerializers(keySer, valSer);
        return (KafkaProducerFactoryBuilder<K2, V2>)this;
    }

    public KafkaProducerFactoryBuilder<K, V> withPartitioner(Class<? extends Partitioner> partitioner) {
        delegate.withPartitioner(partitioner);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withBootstrapServer(String bootStrapServer) {
        delegate.withBootstrapServer(bootStrapServer);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withBootstrapServers(List<String> bootStrapServers) {
        delegate.withBootstrapServers(bootStrapServers);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withClientId(String val) {
        delegate.withClientId(val);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withSecurityProtocol(SecurityProtocol protocol) {
        delegate.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withLingerMs(Duration duration) {
        delegate.withLingerMs(duration);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withBufferMemory(long size) {
        delegate.withBufferMemory(size);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withRequestTimeoutMs(Duration duration) {
        delegate.withRequestTimeoutMs(duration);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withRetryBackoff(Duration duration) {
        delegate.withRetryBackoff(duration);
        return this;
    }

    public KafkaProducerFactoryBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry, String metricsPrefix) {
        delegate.withMetricRegistry(metricRegistry, metricsPrefix);
        return this;
    }
}
