package com.opentable.kafka.spring.builders;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SpringKafkaConsumerFactoryBuilder<K, V>  extends KafkaConsumerBaseBuilder<SpringKafkaConsumerFactoryBuilder<K, V>, K, V> {

    private Optional<Duration> pollTimeout = Optional.empty();
    private Optional<AckMode> ackMode = Optional.empty();


    SpringKafkaConsumerFactoryBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
    }

    @Override
    protected SpringKafkaConsumerFactoryBuilder<K, V> self() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K2, V2> SpringKafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        return (SpringKafkaConsumerFactoryBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K2, V2> SpringKafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        return (SpringKafkaConsumerFactoryBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    public SpringKafkaConsumerFactoryBuilder<K, V> withPollTimeout(Duration pollTimeout) {
        this.pollTimeout = Optional.ofNullable(pollTimeout);
        return this;
    }

    public SpringKafkaConsumerFactoryBuilder<K, V> withAckMode(AckMode ackMode) {
        this.ackMode = Optional.ofNullable(ackMode);
        return this;
    }

    public ConsumerFactory<K, V> buildFactory() {
        return buildFactory(null, null);
    }

    public <K2, V2> ConsumerFactory<K2, V2> buildFactory(@Nullable Deserializer<K2> keyDeserializer,
                                                         @Nullable Deserializer<V2> valueDeserializer) {
        return new DefaultKafkaConsumerFactory<>(buildProperties(), keyDeserializer, valueDeserializer);
    }

    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> build() {
        final ConcurrentKafkaListenerContainerFactory<K, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(buildFactory());
        factory.setConcurrency(1);
        pollTimeout.map(Duration::toMillis).ifPresent(v -> factory.getContainerProperties().setPollTimeout(v));
        ackMode.ifPresent(v -> factory.getContainerProperties().setAckMode(v));
        return factory;
    }
}
