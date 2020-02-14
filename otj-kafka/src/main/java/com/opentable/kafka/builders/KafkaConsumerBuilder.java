/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opentable.kafka.builders;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main builder for KafkaConsumer. This is usually entered via a KafkaConsumerBuilderFactoryBean so some "sugar" is injected.
 * @param <K> Key
 * @param <V> Value
 */
public class KafkaConsumerBuilder<K, V> extends KafkaConsumerBaseBuilder<KafkaConsumerBuilder<K, V>, K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerBuilder.class);

    public KafkaConsumerBuilder(EnvironmentProvider environmentProvider) {
        this(new HashMap<>(), environmentProvider);
    }

    public KafkaConsumerBuilder(Map<String, Object> prop, EnvironmentProvider environmentProvider) {
       super(prop, environmentProvider);
    }

    @Override
    protected KafkaConsumerBuilder<K, V> self() {
        return this;
    }

    @Override
    public <K2, V2> KafkaConsumerBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        return (KafkaConsumerBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    @Override
    public <K2, V2> KafkaConsumerBuilder<K2, V2> withDeserializers(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        return (KafkaConsumerBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    private <CK,CV> Consumer<CK,CV> consumer(Deserializer<CK> keyDeserializer, Deserializer<CV> valuedeserializer) {
        LOG.trace("Building KafkaConsumer with props {}", getFinalProperties());
        if (keyDeserializer != null || valuedeserializer != null) {
            LOG.warn("You passed an INSTANCE (as opposed to class) as a Deserializer. That's fine, but realize configure() will not be called on the Deserializer. That's per Kafka Design.");
        }
        return new KafkaConsumer<>(getFinalProperties(), keyDeserializer, valuedeserializer);
    }

    public Consumer<K, V> build() {
        internalBuild();
        return consumer(keyDeserializerInstance, valueDeserializerInstance);
    }

    public Supplier<Consumer<K, V>> supplier() {
        internalBuild();
        return () -> consumer(keyDeserializerInstance, valueDeserializerInstance);
    }

}
