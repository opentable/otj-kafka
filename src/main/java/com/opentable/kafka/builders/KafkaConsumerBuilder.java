package com.opentable.kafka.builders;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;
import com.opentable.kafka.logging.LoggingInterceptorConfig;

public class KafkaConsumerBuilder <K, V> extends KafkaBuilder {

    KafkaConsumerBuilder(Properties prop) {
        super(prop);
        withLogging();
    }

    @Override
    public KafkaConsumerBuilder<K, V> withProp(Object key, Object value) {
        super.withProp(key, value);
        return this;
    }

    @Override
    public KafkaConsumerBuilder<K, V> withoutProp(Object key) {
        super.withoutProp(key);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLogging() {
        setCsvProp(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLoggingSampleRate(Double rate) {
        return withProp(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, rate);
    }

    public KafkaConsumerBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        setCsvProp(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, clazz.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withGroupId(String val) {
        return withProp(ConsumerConfig.GROUP_ID_CONFIG, val);
    }

    public KafkaConsumerBuilder<K, V> withOffsetReset(AutoOffsetResetType val) {
        return withProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, val.name());
    }

    public KafkaConsumerBuilder<K, V> withMaxPollRecords(int val) {
        return withProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, val);
    }

    public <K2, V2> KafkaConsumerBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSer.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeSer.getName());
        return new KafkaConsumerBuilder<>(prop);
    }

    public KafkaConsumer<K, V> build() {
        // TODO: add checks here
        return new KafkaConsumer<>(prop);
    }

    public enum AutoOffsetResetType {
        latest, earliest, none
    }
}
