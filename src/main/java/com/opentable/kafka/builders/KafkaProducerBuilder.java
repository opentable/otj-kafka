package com.opentable.kafka.builders;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingProducerInterceptor;

public class KafkaProducerBuilder<K, V> extends KafkaBuilder {

    KafkaProducerBuilder(Properties prop) {
        super(prop);
        withLogging();
    }

    @Override
    public KafkaProducerBuilder<K, V> withProp(String key, Object value) {
        super.withProp(key, value);
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> withoutProp(String key) {
        super.withoutProp(key);
        return this;
    }

    public KafkaProducerBuilder<K, V> withLogging() {
        setListPropItem(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withoutLogging() {
        removeListPropItem(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withLoggingSampleRate(Double rate) {
        return withProp(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, rate);
    }

    public KafkaProducerBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        setListPropItem(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, clazz.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withAcks(AckType val) {
        return withProp(ProducerConfig.ACKS_CONFIG, val.value);
    }

    public KafkaProducerBuilder<K, V> withRetries(int val) {
        return withProp(CommonClientConfigs.RETRIES_CONFIG, val);
    }

    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSer);
        return new KafkaProducerBuilder<>(prop);
    }

    public  KafkaProducer<K, V> build() {
        // TODO: add checks here
        return new KafkaProducer<>(prop);
    }

    public enum AckType {
        all("all"),
        none("0"),
        atleastOne("1");

        final String value;

        AckType(String value) {
            this.value = value;
        }
    }

}
