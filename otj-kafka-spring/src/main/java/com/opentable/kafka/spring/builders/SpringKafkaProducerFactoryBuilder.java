package com.opentable.kafka.spring.builders;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaProducerBaseBuilder;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SpringKafkaProducerFactoryBuilder<K, V>  extends KafkaProducerBaseBuilder<SpringKafkaProducerFactoryBuilder<K, V>, K, V> {

    public static final String PRODUCER_PER_CONSUMER_PARTITION_CONFIG = "producer-per-consumer-partition";

    private Optional<String> transactionIdPrefix = Optional.empty();
    private Optional<Boolean> producerPerConsumerPartition = Optional.empty();


    SpringKafkaProducerFactoryBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
        readProperties();
    }

    private void readProperties() {
        readProperty(PRODUCER_PER_CONSUMER_PARTITION_CONFIG, Boolean::valueOf, this::withProducerPerConsumerPartition);
    }

    @Override
    protected SpringKafkaProducerFactoryBuilder<K, V> self() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K2, V2> SpringKafkaProducerFactoryBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        return (SpringKafkaProducerFactoryBuilder<K2, V2>) super.withSerializers(keySer, valSer);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K2, V2> SpringKafkaProducerFactoryBuilder<K2, V2> withSerializers(Serializer<K2> keySer, Serializer<V2> valSer) {
        return (SpringKafkaProducerFactoryBuilder<K2, V2>) super.withSerializers(keySer, valSer);
    }

    /**
     * Set the transactional.id prefix.
     * @param value the prefix.
     */
    public SpringKafkaProducerFactoryBuilder<K, V> withTransactionIdPrefix(String value) {
        transactionIdPrefix = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set to false to revert to the previous behavior of a simple incrementing
     * trasactional.id suffix for each producer instead of maintaining a producer
     * for each group/topic/partition.
     * @param value false to revert.
     */
    public SpringKafkaProducerFactoryBuilder<K, V> withProducerPerConsumerPartition(Boolean value) {
        producerPerConsumerPartition = Optional.ofNullable(value);
        return self();
    }

    /**
     * Construct a factory with the provided configuration.
     */
    public ProducerFactory<K, V> build() {
        return build(null, null);
    }

    /**
     * Construct a factory with the provided configuration and {@link Serializer}s.
     * @param keySerializer the key {@link Serializer}.
     * @param valueSerializer the value {@link Serializer}.
     */
    public <K2, V2> ProducerFactory<K2, V2> build(@Nullable Serializer<K2> keySerializer, @Nullable Serializer<V2> valueSerializer) {
        DefaultKafkaProducerFactory<K2, V2> res = new DefaultKafkaProducerFactory<>(buildProperties(), keySerializer, valueSerializer);
        transactionIdPrefix.ifPresent(res::setTransactionIdPrefix);
        producerPerConsumerPartition.ifPresent(res::setProducerPerConsumerPartition);
        return res;
    }

}
