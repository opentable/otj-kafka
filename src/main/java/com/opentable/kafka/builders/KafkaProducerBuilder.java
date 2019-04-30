package com.opentable.kafka.builders;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingProducerInterceptor;

public class KafkaProducerBuilder<K, V> extends KafkaBuilder<KafkaProducerBuilder<K, V>> {

    KafkaProducerBuilder(Properties prop) {
        super(prop);
        withLogging();
    }

    @Override
    KafkaProducerBuilder<K, V> self() {
        return this;
    }

    public static  KafkaProducerBuilder<?, ?> builder() {
        return new KafkaProducerBuilder<>(new Properties());
    }

    public static KafkaProducerBuilder<?, ?> builder(Properties props) {
        return new KafkaProducerBuilder<>(props);
    }

    public KafkaProducerBuilder<K, V> withLogging() {
        setListPropItem(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> disableLogging() {
        removeListPropItem(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withLoggingSampleRate(Double rate) {
        return withProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, rate);
    }

    public KafkaProducerBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        setListPropItem(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, clazz.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withAcks(AckType val) {
        return withProperty(ProducerConfig.ACKS_CONFIG, val.value);
    }

    public KafkaProducerBuilder<K, V> withRetries(int val) {
        return withProperty(CommonClientConfigs.RETRIES_CONFIG, val);
    }

    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSer);
        return new KafkaProducerBuilder<>(prop);
    }

    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Serializer<K2> keySer, Serializer<V2> valSer) {
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSer);
        return new KafkaProducerBuilder<>(prop);
    }

    public  KafkaProducer<K, V> build() {
        // TODO: add checks here
        return new KafkaProducer<>(prop);
    }

    public <K2, V2> KafkaProducer<K2, V2> build(Serializer<K2> keySer, Serializer<V2> valSer) {
        // TODO: add checks here
        return new KafkaProducer<>(prop, keySer, valSer);
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
