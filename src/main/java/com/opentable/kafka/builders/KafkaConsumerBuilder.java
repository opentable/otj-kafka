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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;

/**
 * Main builder for KafkaConsumer. This is usually entered via a KafkaConsumerBuilderFactoryBean so some "sugar" is injected.
 * @param <K> Key
 * @param <V> Value
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaConsumerBuilder<K, V>  {

    private final KafkaBaseBuilder kafkaBaseBuilder;
    private Optional<String> groupId = Optional.empty();
    private boolean enableAutoCommit = true;
    private OptionalInt maxPollRecords = OptionalInt.empty();
    private OptionalLong sessionTimeoutMs = OptionalLong.empty();
    private OptionalLong maxPollIntervalMs = OptionalLong.empty();
    private AutoOffsetResetType autoOffsetResetType = AutoOffsetResetType.Latest;
    private Class<? extends PartitionAssignor> partitionStrategy = RangeAssignor.class;
    // Kafka is really stupid. In the properties you can only configure a no-args
    // and then they hack around it if you have one supplied
    private Class<? extends Deserializer<K>> keyDe;
    private Class<? extends Deserializer<V>> valueDe;
    private Deserializer<K> keyDeserializerInstance;
    private Deserializer<V> valueDeserializerInstance;

    public KafkaConsumerBuilder(EnvironmentProvider environmentProvider) {
        this(new HashMap<>(), environmentProvider);
    }

    public KafkaConsumerBuilder(Map<String, Object> prop, EnvironmentProvider environmentProvider) {
        kafkaBaseBuilder = new KafkaBaseBuilder(prop, environmentProvider);
    }

    public KafkaConsumerBuilder<K, V> withProperty(String key, Object value) {
        kafkaBaseBuilder.addProperty(key, value);
        return this;
    }

    public KafkaConsumerBuilder<K, V> removeProperty(String key) {
        kafkaBaseBuilder.removeProperty(key);
        return this;
    }

    public KafkaConsumerBuilder<K, V> disableLogging() {
        kafkaBaseBuilder.withLogging(false);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLoggingSampleRate(int rate) {
        kafkaBaseBuilder.withSamplingRatePer10Seconds(rate);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        kafkaBaseBuilder.addInterceptor(clazz.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withGroupId(String val) {
        groupId = Optional.ofNullable(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withAutoOffsetReset(AutoOffsetResetType val) {
        autoOffsetResetType = val == null ? AutoOffsetResetType.None : autoOffsetResetType;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMaxPollRecords(int val) {
        maxPollRecords = OptionalInt.of(val);
        return this;
    }

    /**
     * Provide an class. If you have a no-args constructor use this
     * @param keyDeSer key deserializer
     * @param valDeSer value deserializer
     * @return this
     */
    public KafkaConsumerBuilder<K, V> withDeserializers(Class<? extends Deserializer<K>> keyDeSer, Class<? extends Deserializer<V>> valDeSer) {
        this.keyDe = keyDeSer;
        this.valueDe = valDeSer;
        this.keyDeserializerInstance = null;
        this.valueDeserializerInstance = null;
        return this;
    }

    /**
     * Provide an instance. If you don't have a no-args constructor use this
     * @param keyDeSer key deserializer
     * @param valDeSer value deserializer
     * @return this
     */
    public KafkaConsumerBuilder<K, V> withDeserializers(Deserializer<K> keyDeSer, Deserializer<V> valDeSer) {
        this.keyDeserializerInstance = keyDeSer;
        this.valueDeserializerInstance = valDeSer;
        this.keyDe = null;
        this.valueDe = null;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withPartitionAssignmentStrategy(Class<? extends PartitionAssignor> partitionAssignmentStrategy) {
        partitionStrategy = partitionAssignmentStrategy;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withBootstrapServer(String bootStrapServer) {
        kafkaBaseBuilder.withBootstrapServer(bootStrapServer);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withBootstrapServers(List<String> bootStrapServers) {
        kafkaBaseBuilder.withBootstrapServers(bootStrapServers);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withClientId(String val) {
        kafkaBaseBuilder.withClientId(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withAutoCommit(boolean val) {
        enableAutoCommit = val;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withSecurityProtocol(SecurityProtocol protocol) {
        kafkaBaseBuilder.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withRequestTimeout(Duration duration) {
        if (duration != null) {
            kafkaBaseBuilder.withRequestTimeoutMs(duration);
        }
        return this;
    }

    public KafkaConsumerBuilder<K, V> withRetryBackoff(Duration duration) {
        if (duration != null) {
            kafkaBaseBuilder.withRetryBackOff(duration);
        }
        return this;
    }

    public KafkaConsumerBuilder<K, V> withPollInterval(Duration duration) {
        if (duration != null) {
            maxPollIntervalMs = OptionalLong.of(duration.toMillis());
        }
        return this;
    }

    public KafkaConsumerBuilder<K, V> withSessionTimeoutMs(Duration duration) {
        if (duration != null) {
            sessionTimeoutMs = OptionalLong.of(duration.toMillis());
        }
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry) {
        kafkaBaseBuilder.withMetricRegistry(metricRegistry);
        return this;
    }

    public KafkaConsumer<K, V> build() {
        if (partitionStrategy != null) {
            kafkaBaseBuilder.addProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionStrategy.getName());
        }
        maxPollIntervalMs.ifPresent(m -> kafkaBaseBuilder.addProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(m)));
        sessionTimeoutMs.ifPresent(s -> kafkaBaseBuilder.addProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(s)));
        kafkaBaseBuilder.addProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(enableAutoCommit));
        kafkaBaseBuilder.setupInterceptors(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        groupId.ifPresent(gid -> kafkaBaseBuilder.addProperty(ConsumerConfig.GROUP_ID_CONFIG, gid));
        kafkaBaseBuilder.addProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetType.value);
        maxPollRecords.ifPresent(mpr -> kafkaBaseBuilder.addProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, mpr));
        if (keyDe != null) {
            kafkaBaseBuilder.addProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDe);
        }
        if (valueDe != null) {
            kafkaBaseBuilder.addProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDe);
        }
        // Merge in common and user supplied properties.
        kafkaBaseBuilder.finishBuild();
        kafkaBaseBuilder.cantBeNull(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "Partition assignment strategy can't be null");
        return kafkaBaseBuilder.consumer(keyDeserializerInstance, valueDeserializerInstance);
    }

    public enum AutoOffsetResetType {
        Latest("latest"), Earliest("earliest"), None("none");
        final String value;
        AutoOffsetResetType(String value) {
            this.value = value;
        }

        public static AutoOffsetResetType fromString(String c) {
            return Arrays.stream(values()).filter(t -> t.value.equalsIgnoreCase(c))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("Can't convert " + c));
        }
    }
}
