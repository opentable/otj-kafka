package com.opentable.kafka.builders;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingProducerInterceptor;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class KafkaProducerBaseBuilder<SELF extends KafkaProducerBaseBuilder<SELF, K, V>, K, V> extends KafkaBaseBuilder<SELF> {

    private Optional<KafkaProducerBuilder.AckType> ackType = Optional.empty();
    private OptionalInt retries = OptionalInt.empty();
    private OptionalInt batchSize = OptionalInt.empty();
    private OptionalInt maxInfFlight = OptionalInt.empty();
    private OptionalLong lingerMS = OptionalLong.empty();
    private OptionalLong bufferMemory = OptionalLong.empty();
    private Class<? extends Partitioner> partitioner = DefaultPartitioner.class;
    private Class<? extends Serializer<K>> keySe;
    private Class<? extends Serializer<V>> valueSe;
    protected Serializer<K> keySerializer;
    protected Serializer<V> valueSerializer;

    protected KafkaProducerBaseBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
    }


    public SELF withAcks(KafkaProducerBuilder.AckType val) {
        this.ackType = Optional.ofNullable(val);
        return self();
    }

    public SELF withRetries(int val) {
        this.retries = OptionalInt.of(val);
        return self();
    }

    public SELF withMaxInFlightRequests(int val) {
        this.maxInfFlight = OptionalInt.of(val);
        return self();
    }

    public SELF withBatchSize(int val) {
        this.batchSize = OptionalInt.of(val);
        return self();
    }

    /**
     * Provide an class. If you have a no-args constructor use this
     * @param keySer key serializer
     * @param valSer value serializer
     * @param <K2> The type of the key expected by serializer
     * @param <V2> The type of the value expected by serializer
     * @return this
     */
    @SuppressWarnings("unchecked")
    protected <K2, V2> KafkaProducerBaseBuilder<?, K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        KafkaProducerBaseBuilder<? extends SELF, K2, V2> res = (KafkaProducerBaseBuilder<? extends SELF, K2, V2>) this;
        res.keySe =  keySer;
        res.valueSe =  valSer;
        res.keySerializer = null;
        res.valueSerializer = null;
        return res;
    }

    /**
     * Provide an instance. If you don't have a no-args constructor use this
     * @param keySer key serializer
     * @param valSer value serializer
     * @param <K2> The type of the key expected by serializer
     * @param <V2> The type of the value expected by serializer
     * @return this
     */
    @SuppressWarnings("unchecked")
    protected <K2, V2> KafkaProducerBaseBuilder<?, K2, V2> withSerializers(Serializer<K2> keySer, Serializer<V2> valSer) {
        KafkaProducerBaseBuilder<? extends SELF, K2, V2> res = (KafkaProducerBaseBuilder<? extends SELF, K2, V2>) this;
        res.keySerializer = keySer;
        res.valueSerializer = valSer;
        res.keySe = null;
        res.valueSe = null;
        return res;
    }

    public SELF withPartitioner(Class<? extends Partitioner> partitioner) {
        if (partitioner != null) {
            this.partitioner = partitioner;
        }
        return self();
    }


    public SELF withLingerMs(Duration duration) {
        if (duration != null) {
            lingerMS = OptionalLong.of(duration.toMillis());
        } else {
            lingerMS = OptionalLong.empty();
        }
        return self();
    }

    public SELF withBufferMemory(long size) {
        this.bufferMemory = OptionalLong.of(size);
        return self();
    }


    public SELF withMetricRegistry(MetricRegistry metricRegistry, String metricsPrefix) {
        this.withMetricRegistry(metricRegistry);
        this.withPrefix(metricsPrefix);
        return self();
    }

    public SELF withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        this.addInterceptor(clazz.getName());
        return self();
    }

    protected void internalBuild() {
        this.addProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.getName());
        this.setupInterceptors(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        maxInfFlight.ifPresent(m -> this.addProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, m));
        bufferMemory.ifPresent(b -> this.addProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, b));
        batchSize.ifPresent(b -> this.addProperty(ProducerConfig.BATCH_SIZE_CONFIG, b));
        lingerMS.ifPresent(l -> this.addProperty(ProducerConfig.LINGER_MS_CONFIG, l));
        ackType.ifPresent(ack -> this.addProperty(ProducerConfig.ACKS_CONFIG, ack.value));
        retries.ifPresent(retries -> this.addProperty(CommonClientConfigs.RETRIES_CONFIG, retries));
        if (keySe != null) {
            this.addProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySe.getName());
        }
        if (valueSe != null) {
            this.addProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSe.getName());
        }

        // merge in common and seed properties
        this.finishBuild();
    }

    public Map<String, Object> buildProperties() {
        internalBuild();
        return this.getFinalProperties();
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
