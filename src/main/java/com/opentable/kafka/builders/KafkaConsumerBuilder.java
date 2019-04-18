package com.opentable.kafka.builders;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;
import com.opentable.service.AppInfo;

/**
 * Main builder for KafkaConsumer. This is usually entered via a KafkaConsumerBuilderFactoryBean so some "sugar" is injected.
 * @param <K>
 * @param <V>
 */
public class KafkaConsumerBuilder<K, V>  {

    private final KafkaBaseBuilder kafkaBaseBuilder;
    private Optional<String> groupId = Optional.empty();
    private Optional<Integer> maxPollRecords = Optional.empty();
    private AutoOffsetResetType autoOffsetResetType = AutoOffsetResetType.Latest;
    private Class<? extends Deserializer<K>> keyDe;
    private Class<? extends Deserializer<V>> valueDe;

    public KafkaConsumerBuilder(Properties prop, AppInfo appInfo) {
        kafkaBaseBuilder = new KafkaBaseBuilder(prop, appInfo);
        kafkaBaseBuilder.interceptors.add(LoggingConsumerInterceptor.class.getName());
    }

    public KafkaConsumerBuilder<K, V> withProperty(String key, Object value) {
        kafkaBaseBuilder.addProperty(key, value);
        return this;
    }

    public KafkaConsumerBuilder<K, V> removeProperty(String key) {
        kafkaBaseBuilder.removeProperty(key);
        return this;
    }

    public KafkaConsumerBuilder<K, V> disableLogging() {
        kafkaBaseBuilder.interceptors.remove(LoggingConsumerInterceptor.class.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLoggingSampleRate(double rate) {
        kafkaBaseBuilder.loggingSampleRate = rate;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        kafkaBaseBuilder.interceptors.add(clazz.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withGroupId(String val) {
        groupId = Optional.ofNullable(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withOffsetReset(AutoOffsetResetType val) {
        autoOffsetResetType = val == null ? AutoOffsetResetType.None : autoOffsetResetType;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMaxPollRecords(int val) {
        maxPollRecords = Optional.of(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withDeserializers(Class<? extends Deserializer<K>> keyDeSer, Class<? extends Deserializer<V>> valDeSer) {
        this.keyDe = keyDeSer;
        this.valueDe = valDeSer;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withBootstrapServer(String bootStrapServer) {
        kafkaBaseBuilder.withBootstrapServer(bootStrapServer);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withBootstrapServers(List<String> bootStrapServers) {
        kafkaBaseBuilder.withBootstrapServers(bootStrapServers);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withClientId(String val) {
        kafkaBaseBuilder.withClientId(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withSecurityProtocol(String protocol) {
        kafkaBaseBuilder.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry) {
        kafkaBaseBuilder.withMetricRegistry(metricRegistry);
        return this;
    }

    public KafkaConsumer<K, V> build() {
        kafkaBaseBuilder.baseBuild();
        kafkaBaseBuilder.addLoggingUtilsRef(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        // Isn't this mandatory?
        groupId.ifPresent(gid -> kafkaBaseBuilder.addProperty(ConsumerConfig.GROUP_ID_CONFIG, gid));
        kafkaBaseBuilder.addProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetType.value);
        maxPollRecords.ifPresent(mpr -> kafkaBaseBuilder.addProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, mpr));
        if (keyDe == null || valueDe == null) {
            throw new IllegalStateException("Either keyDeserializer or ValueDeserializer is missing");
        }
        kafkaBaseBuilder.addProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDe);
        kafkaBaseBuilder.addProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDe);

        return kafkaBaseBuilder.consumer();
    }

    public enum AutoOffsetResetType {
        Latest("latest"), Earliest("earliest"), None("none");
        final String value;
        AutoOffsetResetType(String value) {
            this.value = value;
        }
    }
}
