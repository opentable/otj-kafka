package com.opentable.kafka.builders;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingProducerInterceptor;

public class KafkaProducerBuilder<K, V> extends KafkaBuilder {

    KafkaProducerBuilder(Properties prop) {
        super(prop);
        withLogging();
    }

    @Override
    public KafkaProducerBuilder<K, V> withProp(Object key, Object value) {
        super.withProp(key, value);
        return this;
    }

    @Override
    public KafkaProducerBuilder<K, V> withoutProp(Object key) {
        super.withoutProp(key);
        return this;
    }

    public KafkaProducerBuilder<K, V> withLogging() {
        setCsvProp(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        setCsvProp(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, clazz.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withAcks(String val) {
        return withProp(ProducerConfig.ACKS_CONFIG, val);
    }

    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSer.getName());
        return new KafkaProducerBuilder<>(prop);
    }

    public  KafkaProducer<K, V> build() {
        // TODO: add checks here
        return new KafkaProducer<>(prop);
    }

}
