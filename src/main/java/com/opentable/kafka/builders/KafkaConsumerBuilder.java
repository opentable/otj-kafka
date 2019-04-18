package com.opentable.kafka.builders;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;
import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.service.AppInfo;

public class KafkaConsumerBuilder<K, V> extends KafkaBaseBuilder {
    private Optional<String> groupId = Optional.empty();
    private Optional<Integer> maxPollRecords = Optional.empty();
    private AutoOffsetResetType autoOffsetResetType = AutoOffsetResetType.Latest;
    private Class<? extends Deserializer<K>> keyDe;
    private Class<? extends Deserializer<V>> valueDe;

    public KafkaConsumerBuilder(Properties prop, AppInfo appInfo) {
        super(prop, appInfo);
        interceptors.add(LoggingConsumerInterceptor.class.getName());
    }

    public KafkaConsumerBuilder<K, V> withProperty(String key, Object value) {
        super.addProperty(key, value);
        return this;
    }

    public KafkaConsumerBuilder<K, V> removeProperty(String key) {
        super.removeProperty(key);
        return this;
    }

    public KafkaConsumerBuilder<K, V> disableLogging() {
        interceptors.remove(LoggingConsumerInterceptor.class.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLoggingSampleRate(double rate) {
        loggingSampleRate = rate;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        interceptors.add(clazz.getName());
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
        super.withBootstrapServer(bootStrapServer);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withBootstrapServers(List<String> bootStrapServers) {
        super.withBootstrapServers(bootStrapServers);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withClientId(String val) {
        super.withClientId(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withSecurityProtocol(String protocol) {
        super.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry) {
        super.withMetricRegistry(metricRegistry);
        return this;
    }


    public KafkaConsumer<K, V> build() {
        baseBuild();
        if (!interceptors.isEmpty()) {
            addProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors.stream().distinct().collect(Collectors.joining(",")));
            if (interceptors.contains(LoggingConsumerInterceptor.class.getName())) {
                addProperty("opentable.logging",  loggingUtils);
            }
        }
        addProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, loggingSampleRate);
        groupId.ifPresent(gid -> addProperty(ConsumerConfig.GROUP_ID_CONFIG, gid));
        addProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetType.value);
        maxPollRecords.ifPresent(mpr -> addProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, mpr));
        if (keyDe == null || valueDe == null) {
            throw new IllegalStateException("Either keyDeserializer or ValueDeserializer is missing");
        }
        addProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDe);
        addProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDe);
        return new KafkaConsumer<>(prop);
    }

    public enum AutoOffsetResetType {
        Latest("latest"), Earliest("earliest"), None("none");
        final String value;
        AutoOffsetResetType(String value) {
            this.value = value;
        }
    }
}
