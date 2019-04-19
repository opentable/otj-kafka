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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.Serializer;

import com.opentable.kafka.logging.LoggingProducerInterceptor;
import com.opentable.service.AppInfo;

public class KafkaProducerBuilder<K, V> {

    private final KafkaBaseBuilder kafkaBaseBuilder;
    private Optional<AckType> ackType = Optional.empty();
    private OptionalInt retries = OptionalInt.empty();
    private int maxInfFlight = 5;
    private Class<? extends Partitioner> partitioner = DefaultPartitioner.class;

    private Class<? extends Serializer<K>> keySe;
    private Class<? extends Serializer<V>> valueSe;


    public KafkaProducerBuilder(Map<String, Object> prop, AppInfo appInfo) {
        kafkaBaseBuilder = new KafkaBaseBuilder(prop, appInfo);
        kafkaBaseBuilder.addInterceptor(LoggingProducerInterceptor.class.getName());
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
        kafkaBaseBuilder.removeInterceptor(LoggingProducerInterceptor.class.getName());
        return this;
    }

    public KafkaProducerBuilder<K, V> withLoggingSampleRate(int rate) {
        kafkaBaseBuilder.withSamplingRatePer10Seconds(rate);
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
        this.maxInfFlight = val;
        return this;
    }

    public KafkaProducerBuilder<K, V> withSerializers(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valSer) {
        this.keySe = keySer;
        this.valueSe = valSer;
        return this;
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

    public KafkaProducerBuilder<K, V> withSecurityProtocol(String protocol) {
        kafkaBaseBuilder.withSecurityProtocol(protocol);
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

    public KafkaProducerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry) {
        kafkaBaseBuilder.withMetricRegistry(metricRegistry);
        return this;
    }

    public KafkaProducer<K, V> build() {
        kafkaBaseBuilder.addProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.getName());
        kafkaBaseBuilder.addLoggingUtilsRef(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
        kafkaBaseBuilder.addProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInfFlight);
        ackType.ifPresent(ack -> kafkaBaseBuilder.addProperty(ProducerConfig.ACKS_CONFIG, ack.value));
        retries.ifPresent(retries -> kafkaBaseBuilder.addProperty(CommonClientConfigs.RETRIES_CONFIG, retries));
        if (keySe != null) {
            kafkaBaseBuilder.addProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySe);
        }
        if (valueSe != null) {
            kafkaBaseBuilder.addProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSe);
        }

        // merge in common and seed properties
        kafkaBaseBuilder.finishBuild();
        return kafkaBaseBuilder.producer();
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
