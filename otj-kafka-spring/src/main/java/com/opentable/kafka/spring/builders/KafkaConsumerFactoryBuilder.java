package com.opentable.kafka.spring.builders;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaConsumerBuilder;

public class KafkaConsumerFactoryBuilder<K, V>  {

    private final KafkaConsumerBuilder<K, V> delegate;

    public KafkaConsumerFactoryBuilder(EnvironmentProvider environmentProvider) {
        this.delegate = new KafkaConsumerBuilder<>(environmentProvider);
    }

    public KafkaConsumerFactoryBuilder(Map<String, Object> prop, EnvironmentProvider environmentProvider) {
        this.delegate = new KafkaConsumerBuilder<>(prop, environmentProvider);
    }

    public ConsumerFactory<K, V> build() {
        return build(null, null);
    }

    public <K2, V2> ConsumerFactory<K2, V2> build(@Nullable Deserializer<K2> keyDeserializer,
                                       @Nullable Deserializer<V2> valueDeserializer) {
        return new DefaultKafkaConsumerFactory<>(delegate.buildProperties(), keyDeserializer, valueDeserializer);
    }

    public KafkaConsumerFactoryBuilder<K, V> withProperty(String key, Object value) {
        delegate.withProperty(key, value);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withProperties(Map<String, Object> config) {
        delegate.withProperties(config);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> removeProperty(String key) {
        delegate.removeProperty(key);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> disableLogging() {
        delegate.disableLogging();
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withSamplingRatePer10Seconds(int rate) {
        delegate.withSamplingRatePer10Seconds(rate);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withRandomSamplingRate(int rate) {
        delegate.withRandomSamplingRate(rate);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        delegate.withInterceptor(clazz);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withGroupId(String val) {
        delegate.withGroupId(val);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withAutoOffsetReset(KafkaConsumerBuilder.AutoOffsetResetType val) {
        delegate.withAutoOffsetReset(val);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withMaxPollRecords(int val) {
        delegate.withMaxPollRecords(val);
        return this;
    }

    public <K2, V2> KafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        delegate.withDeserializers(keyDeSer, valDeSer);
        return (KafkaConsumerFactoryBuilder<K2, V2>)this;
    }

    public <K2, V2> KafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        delegate.withDeserializers(keyDeSer, valDeSer);
        return (KafkaConsumerFactoryBuilder<K2, V2>)this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withPartitionAssignmentStrategy(Class<? extends PartitionAssignor> partitionAssignmentStrategy) {
        delegate.withPartitionAssignmentStrategy(partitionAssignmentStrategy);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withBootstrapServer(String bootStrapServer) {
        delegate.withBootstrapServer(bootStrapServer);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withBootstrapServers(List<String> bootStrapServers) {
        delegate.withBootstrapServers(bootStrapServers);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withClientId(String val) {
        delegate.withClientId(val);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withAutoCommit(boolean val) {
        delegate.withAutoCommit(val);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withSecurityProtocol(SecurityProtocol protocol) {
        delegate.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withRequestTimeout(Duration duration) {
        delegate.withRequestTimeout(duration);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withMaxPartitionFetchBytes(int bytes) {
        delegate.withMaxPartitionFetchBytes(bytes);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withRetryBackoff(Duration duration) {
        delegate.withRetryBackoff(duration);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withPollInterval(Duration duration) {
        delegate.withPollInterval(duration);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withSessionTimeoutMs(Duration duration) {
        delegate.withSessionTimeoutMs(duration);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry, String metricsPrefix) {
        delegate.withMetricRegistry(metricRegistry, metricsPrefix);
        return this;
    }

    public KafkaConsumerFactoryBuilder<K, V> disableMetrics() {
        delegate.disableMetrics();
        return this;
    }

}
