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

import java.util.Map;
import java.util.function.Supplier;

import javax.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * TODO: Food for thought
 * For supporting Kafka Streams. A work in progress
 * - See original DefaultKafkaClientSupplier
 * - needs testing
 * - naming is dubious, sigh
 * - expose as spring bean
 * - metrics namespace stability
 * - Inability to edit properties EXCEPT via Map, which isn't consistent with others
 * (The "fix" to this is expose a composite builder delegating to both consumer and producer builders, but
 * that seems messy)
 * KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(config), supplier);
 */
public class OTKafkaClientSupplier implements KafkaClientSupplier {
    private final KafkaBuilderFactoryBean kafkaBuilderFactoryBean;
    private final Supplier<String> consumerNamer;
    private final Supplier<String> producerNamer;


    @Inject
    public OTKafkaClientSupplier(KafkaBuilderFactoryBean kafkaBuilderFactoryBean, Supplier<String> consumerNamer, Supplier<String> producerNamer) {
        this.kafkaBuilderFactoryBean = kafkaBuilderFactoryBean;
        this.consumerNamer = consumerNamer;
        this.producerNamer = producerNamer;
    }

    @Override
    public Admin getAdmin(final Map<String, Object> config) {
        // create a new client upon each call; but expect this call to be only triggered once so this should be fine
        return AdminClient.create(config);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
        return kafkaBuilderFactoryBean.producerBuilder(consumerNamer.get())
                .withProperties(config)
                .withSerializers(ByteArraySerializer.class, ByteArraySerializer.class)
                .build();

    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        return kafkaBuilderFactoryBean.consumerBuilder(producerNamer.get())
                .withProperties(config)
                .withDeserializers(ByteArrayDeserializer.class, ByteArrayDeserializer.class)
                .build();
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
        return getConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
        return getConsumer(config);
    }

}
