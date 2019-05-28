package com.opentable.kafka.spring.builders;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;

public class KafkaConsumerFactoryBuilder<K, V>  extends KafkaConsumerBaseBuilder<KafkaConsumerFactoryBuilder<K, V>, K, V> {


    KafkaConsumerFactoryBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
    }

    @Override
    protected KafkaConsumerFactoryBuilder<K, V> self() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K2, V2> KafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        return (KafkaConsumerFactoryBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K2, V2> KafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        return (KafkaConsumerFactoryBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
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
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;

    }
}
