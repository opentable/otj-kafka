package com.opentable.kafka.builders;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingProducerInterceptor;
import com.opentable.service.AppInfo;

public class KafkaProducerBuilder<K,V> extends KafkaBaseBuilder {

    private Optional<AckType> ackType = Optional.empty();
    private Optional<Integer> retries = Optional.empty();

    private Class<? extends Serializer<K>> keySe;
    private Class<? extends Serializer<V>> valueSe;


    public KafkaProducerBuilder(Properties prop, AppInfo appInfo) {
        super(prop, appInfo);
        interceptors.add(LoggingProducerInterceptor.class.getName());
    }

    public KafkaProducerBuilder<K, V> withProperty(String key, Object value) {
        super.addProperty(key, value);
        return this;
    }

    public KafkaProducerBuilder<K, V> removeProperty(String key) {
        super.removeProperty(key);
        return this;
    }


    public KafkaProducerBuilder<K, V> disableLogging() {
        interceptors.remove(LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withLoggingSampleRate(double rate) {
        loggingSampleRate = rate;
        return this;
    }

    public KafkaProducerBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        interceptors.add(clazz.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withAcks(AckType val) {
        this.ackType = Optional.ofNullable(val);
        return this;
    }

    public KafkaProducerBuilder<K, V> withRetries(int val) {
        this.retries = Optional.of(val);
        return this;
    }

    public KafkaProducerBuilder<K, V> withSerializers(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valSer) {
        this.keySe = keySer;
        this.valueSe = valSer;
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSer);
        return this;
    }

    public  KafkaProducer<K, V> build() {
        baseBuild();
        if (!interceptors.isEmpty()) {
            addProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors.stream().distinct().collect(Collectors.joining(",")));
            if (interceptors.contains(LoggingProducerInterceptor.class.getName())) {
                addProperty("opentable.logging",  loggingUtils);
            }
        }
        addProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, loggingSampleRate);
        ackType.ifPresent(ack -> addProperty(ProducerConfig.ACKS_CONFIG, ack.value));
        retries.ifPresent(retries -> addProperty(CommonClientConfigs.RETRIES_CONFIG, retries));
        if (keySe == null || valueSe == null) {
            throw new IllegalStateException("Either keySerializer or valueSerializer is missing");
        }
        addProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySe);
        addProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSe);
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
