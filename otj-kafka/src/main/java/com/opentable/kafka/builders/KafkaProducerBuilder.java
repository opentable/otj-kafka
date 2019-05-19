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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingProducerInterceptor;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaProducerBuilder<K, V> {

    private final KafkaBaseBuilder kafkaBaseBuilder;
    private Optional<AckType> ackType = Optional.empty();
    private OptionalInt retries = OptionalInt.empty();
    private OptionalInt batchSize = OptionalInt.empty();
    private OptionalInt maxInfFlight = OptionalInt.empty();
    private OptionalLong lingerMS = OptionalLong.empty();
    private OptionalLong bufferMemory = OptionalLong.empty();
    private Class<? extends Partitioner> partitioner = DefaultPartitioner.class;
    private Class<? extends Serializer<K>> keySe;
    private Class<? extends Serializer<V>> valueSe;
    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;


    // Constructors

    public KafkaProducerBuilder(EnvironmentProvider environmentProvider) {
        this(new HashMap<>(), environmentProvider);
    }

    public KafkaProducerBuilder(Map<String, Object> prop, EnvironmentProvider environmentProvider) {
        kafkaBaseBuilder = new KafkaBaseBuilder(prop, environmentProvider);
    }


    // builder methods

    public KafkaProducerBuilder<K, V> withProperty(String key, Object value) {
        kafkaBaseBuilder.addProperty(key, value);
        return this;
    }

    public KafkaProducerBuilder<K,V> withProperties(final Map<String, Object> config) {
        this.kafkaBaseBuilder.addProperties(config);
        return this;
    }

    public KafkaProducerBuilder<K, V> removeProperty(String key) {
        kafkaBaseBuilder.removeProperty(key);
        return this;
    }


    public KafkaProducerBuilder<K, V> disableLogging() {
        kafkaBaseBuilder.withLogging(false);
        return this;
    }

    public KafkaProducerBuilder<K, V> disableMetrics() {
        kafkaBaseBuilder.withMetrics(false);
        return this;
    }


    public KafkaProducerBuilder<K, V> withSamplingRatePer10Seconds(int rate) {
        kafkaBaseBuilder.withSamplingRatePer10Seconds(rate);
        return this;
    }

    public KafkaProducerBuilder<K, V> withRandomSamplingRate(int rate) {
        kafkaBaseBuilder.withRandomSamplingRate(rate);
        return this;
    }

    public KafkaProducerBuilder<K, V> withInterceptor(Class<? extends ProducerInterceptor<K, V>> clazz) {
        kafkaBaseBuilder.addInterceptor(clazz.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withAcks(AckType val) {
        this.ackType = Optional.ofNullable(val);
        return this;
    }

    public KafkaProducerBuilder<K, V> withRetries(int val) {
        this.retries = OptionalInt.of(val);
        return this;
    }

    public KafkaProducerBuilder<K, V> withMaxInFlightRequests(int val) {
        this.maxInfFlight = OptionalInt.of(val);
        return this;
    }

    public KafkaProducerBuilder<K, V> withBatchSize(int val) {
        this.batchSize = OptionalInt.of(val);
        return this;
    }

    /**
     * Provide an class. If you have a no-args constructor use this
     * @param keySer key serializer
     * @param valSer value serializer
     * @param <K2> The type of the key expected by serializer
     * @param <V2> The type of the value expected by serializer
     * @return this
     */
    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Class<? extends Serializer<K2>> keySer, Class<? extends Serializer<V2>> valSer) {
        KafkaProducerBuilder<K2, V2> res = (KafkaProducerBuilder<K2, V2>) this;
        res.keySe = keySer;
        res.valueSe = valSer;
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
    public <K2, V2> KafkaProducerBuilder<K2, V2> withSerializers(Serializer<K2> keySer, Serializer<V2> valSer) {
        KafkaProducerBuilder<K2, V2> res = (KafkaProducerBuilder<K2, V2>) this;
        res.keySerializer = keySer;
        res.valueSerializer = valSer;
        res.keySe = null;
        res.valueSe = null;
        return res;
    }

    public KafkaProducerBuilder<K, V> withPartitioner(Class<? extends Partitioner> partitioner) {
        if (partitioner != null) {
            this.partitioner = partitioner;
        }
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

    public KafkaProducerBuilder<K, V> withSecurityProtocol(SecurityProtocol protocol) {
        kafkaBaseBuilder.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaProducerBuilder<K, V> withLingerMs(Duration duration) {
        if (duration != null) {
            lingerMS = OptionalLong.of(duration.toMillis());
        } else {
            lingerMS = OptionalLong.empty();
        }
        return this;
    }

    public KafkaProducerBuilder<K, V> withBufferMemory(long size) {
        this.bufferMemory = OptionalLong.of(size);
        return this;
    }

    public KafkaProducerBuilder<K, V> withRequestTimeoutMs(Duration duration) {
        if (duration != null) {
            kafkaBaseBuilder.withRequestTimeoutMs(duration);
        }
        return this;
    }

    public KafkaProducerBuilder<K, V> withRetryBackoff(Duration duration) {
        if (duration != null) {
            kafkaBaseBuilder.withRetryBackOff(duration);
        }
        return this;
    }

    public KafkaProducerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry, String metricsPrefix) {
        kafkaBaseBuilder.withMetricRegistry(metricRegistry);
        kafkaBaseBuilder.withPrefix(metricsPrefix);
        return this;
    }

    /**
     * Build the producer.
     * @return kafka producer
     */
    public Producer<K, V> build() {
        kafkaBaseBuilder.addProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.getName());
        kafkaBaseBuilder.setupInterceptors(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        maxInfFlight.ifPresent(m -> kafkaBaseBuilder.addProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, m));
        bufferMemory.ifPresent(b -> kafkaBaseBuilder.addProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, b));
        batchSize.ifPresent(b -> kafkaBaseBuilder.addProperty(ProducerConfig.BATCH_SIZE_CONFIG, b));
        lingerMS.ifPresent(l -> kafkaBaseBuilder.addProperty(ProducerConfig.LINGER_MS_CONFIG, l));
        ackType.ifPresent(ack -> kafkaBaseBuilder.addProperty(ProducerConfig.ACKS_CONFIG, ack.value));
        retries.ifPresent(retries -> kafkaBaseBuilder.addProperty(CommonClientConfigs.RETRIES_CONFIG, retries));
        if (keySe != null) {
            kafkaBaseBuilder.addProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySe.getName());
        }
        if (valueSe != null) {
            kafkaBaseBuilder.addProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSe.getName());
        }

        // merge in common and seed properties
        kafkaBaseBuilder.finishBuild();
        return kafkaBaseBuilder.producer(keySerializer, valueSerializer);
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

    @VisibleForTesting
    KafkaBaseBuilder getKafkaBaseBuilder() {
        return kafkaBaseBuilder;
    }
}
