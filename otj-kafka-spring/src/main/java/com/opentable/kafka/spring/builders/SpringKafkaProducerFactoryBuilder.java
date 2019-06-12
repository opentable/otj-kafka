package com.opentable.kafka.spring.builders;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaProducerBaseBuilder;

public class SpringKafkaProducerFactoryBuilder<K, V>  extends KafkaProducerBaseBuilder<SpringKafkaProducerFactoryBuilder<K, V>, K, V> {


    SpringKafkaProducerFactoryBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
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

    public ProducerFactory<K, V> build() {
        return build(null, null);
    }

    public <K2, V2> ProducerFactory<K2, V2> build(@Nullable Serializer<K2> keySerializer, @Nullable Serializer<V2> valueSerializer) {
        return new DefaultKafkaProducerFactory<>(buildProperties(), keySerializer, valueSerializer);
    }

}
