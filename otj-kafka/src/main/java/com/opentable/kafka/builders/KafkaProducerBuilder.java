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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main builder for KafkaProducer. This is usually entered via a KafkaConsumerBuilderFactoryBean so some "sugar" is injected.
 * @param <K> Key
 * @param <V> Value
 */
public class KafkaProducerBuilder<K, V>  extends KafkaProducerBaseBuilder<KafkaProducerBuilder<K, V>, K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBaseBuilder.class);

    public KafkaProducerBuilder(EnvironmentProvider environmentProvider) {
        this(new HashMap<>(), environmentProvider);
    }

    public KafkaProducerBuilder(Map<String, Object> prop, EnvironmentProvider environmentProvider) {
        super(prop, environmentProvider);
    }

    @Override
    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        return (KafkaProducerBuilder<K2, V2>) super.withSerializers(keySer, valSer);
    }

    @Override
    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Serializer<K2> keySer, Serializer<V2> valSer) {
        return (KafkaProducerBuilder<K2, V2>) super.withSerializers(keySer, valSer);
    }

    @Override
    protected KafkaProducerBuilder<K, V> self() {
        return this;
    }

    private <PK,PV> Producer<PK,PV> producer(Serializer<PK> keySerializer, Serializer<PV> valueSerializer) {
        LOG.trace("Building KafkaProducer with props {}", getFinalProperties());
        if (keySerializer != null || valueSerializer != null) {
            LOG.warn("You passed an INSTANCE (as opposed to class) as a Serializer. That's fine, but realize configure() will not be called on the Serializer. That's per Kafka Design.");
        }
        return new KafkaProducer<>(getFinalProperties(), keySerializer, valueSerializer);
    }

    public Producer<K, V> build() {
        internalBuild();
        return this.producer(keySerializer, valueSerializer);
    }

}
