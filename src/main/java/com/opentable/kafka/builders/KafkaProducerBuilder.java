package com.opentable.kafka.builders;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingProducerInterceptor;
import com.opentable.service.AppInfo;

public class KafkaProducerBuilder<K,V>  {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerBuilder.class);

    private final KafkaBaseBuilder kafkaBaseBuilder;
    private Optional<AckType> ackType = Optional.empty();
    private Optional<Integer> retries = Optional.empty();

    private Class<? extends Serializer<K>> keySe;
    private Class<? extends Serializer<V>> valueSe;


    public KafkaProducerBuilder(Properties prop, AppInfo appInfo) {
        kafkaBaseBuilder = new KafkaBaseBuilder(prop, appInfo);
        kafkaBaseBuilder.interceptors.add(LoggingProducerInterceptor.class.getName());
    }

    public KafkaProducerBuilder<K, V> withProperty(String key, Object value) {
        kafkaBaseBuilder.addProperty(key, value);
        return this;
    }

    public KafkaProducerBuilder<K, V> removeProperty(String key) {
        kafkaBaseBuilder.removeProperty(key);
        return this;
    }


    public KafkaProducerBuilder<K, V> disableLogging() {
        kafkaBaseBuilder.interceptors.remove(LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withLoggingSampleRate(double rate) {
        kafkaBaseBuilder.loggingSampleRate = rate;
        return this;
    }

    public KafkaProducerBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        kafkaBaseBuilder.interceptors.add(clazz.getName());
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
        return this;
    }

    public KafkaProducerBuilder<K, V> withBootstrapServer(String bootStrapServer) {
        kafkaBaseBuilder.withBootstrapServer(bootStrapServer);
        return this;
    }

    public KafkaProducerBuilder<K, V> withBootstrapServers(List<String> bootStrapServers) {
        kafkaBaseBuilder.withBootstrapServers(bootStrapServers);
        return this;
    }

    public KafkaProducerBuilder<K, V> withClientId(String val) {
        kafkaBaseBuilder.withClientId(val);
        return this;
    }

    public KafkaProducerBuilder<K, V> withSecurityProtocol(String protocol) {
        kafkaBaseBuilder.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaProducerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry) {
        kafkaBaseBuilder.withMetricRegistry(metricRegistry);
        return this;
    }

    public  KafkaProducer<K, V> build() {
        kafkaBaseBuilder.baseBuild();
        if (!kafkaBaseBuilder.interceptors.isEmpty()) {
            kafkaBaseBuilder.addProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, kafkaBaseBuilder.interceptors.stream().distinct().collect(Collectors.joining(",")));
            if (kafkaBaseBuilder.interceptors.contains(LoggingProducerInterceptor.class.getName())) {
                kafkaBaseBuilder.addProperty("opentable.logging",  kafkaBaseBuilder.loggingUtils);
            }
        }
        kafkaBaseBuilder.addProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, kafkaBaseBuilder.loggingSampleRate);
        ackType.ifPresent(ack -> kafkaBaseBuilder.addProperty(ProducerConfig.ACKS_CONFIG, ack.value));
        retries.ifPresent(retries -> kafkaBaseBuilder.addProperty(CommonClientConfigs.RETRIES_CONFIG, retries));
        if (keySe == null || valueSe == null) {
            throw new IllegalStateException("Either keySerializer or valueSerializer is missing");
        }
        kafkaBaseBuilder.addProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySe);
        kafkaBaseBuilder.addProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSe);
        LOG.trace("Building KafkaProducer with props {}", kafkaBaseBuilder.prop);
        return new KafkaProducer<>(kafkaBaseBuilder.prop);
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
