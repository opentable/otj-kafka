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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;
import com.opentable.service.AppInfo;

/**
 * Main builder for KafkaConsumer. This is usually entered via a KafkaConsumerBuilderFactoryBean so some "sugar" is injected.
 * @param <K>
 * @param <V>
 */
public class KafkaConsumerBuilder<K, V>  {

    private final KafkaBaseBuilder kafkaBaseBuilder;
    private Optional<String> groupId = Optional.empty();
    private Optional<Integer>maxPollRecords = Optional.empty();
    private AutoOffsetResetType autoOffsetResetType = AutoOffsetResetType.Latest;
    private Class<? extends Deserializer<K>> keyDe;
    private Class<? extends Deserializer<V>> valueDe;

    public KafkaConsumerBuilder(Map<String, Object> prop, AppInfo appInfo) {
        kafkaBaseBuilder = new KafkaBaseBuilder(prop, appInfo);
        kafkaBaseBuilder.interceptors.add(LoggingConsumerInterceptor.class.getName());
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
        kafkaBaseBuilder.interceptors.remove(LoggingConsumerInterceptor.class.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLoggingSampleRate(double rate) {
        kafkaBaseBuilder.loggingSampleRate = rate;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        kafkaBaseBuilder.interceptors.add(clazz.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withGroupId(String val) {
        groupId = Optional.ofNullable(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withOffsetReset(AutoOffsetResetType val) {
        autoOffsetResetType = val == null ? AutoOffsetResetType.None : autoOffsetResetType;
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMaxPollRecords(int val) {
        maxPollRecords = Optional.of(val);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withDeserializers(Class<? extends Deserializer<K>> keyDeSer, Class<? extends Deserializer<V>> valDeSer) {
        this.keyDe = keyDeSer;
        this.valueDe = valDeSer;
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

    public KafkaConsumerBuilder<K, V> withSecurityProtocol(String protocol) {
        kafkaBaseBuilder.withSecurityProtocol(protocol);
        return this;
    }

    public KafkaConsumerBuilder<K, V> withMetricRegistry(MetricRegistry metricRegistry) {
        kafkaBaseBuilder.withMetricRegistry(metricRegistry);
        return this;
    }

    public KafkaConsumer<K, V> build() {
        kafkaBaseBuilder.addLoggingUtilsRef(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        // Isn't this mandatory?
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
        kafkaBaseBuilder.cantBeNull(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Key deserializer missing");
        kafkaBaseBuilder.cantBeNull(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Value deserializer missing");
        return kafkaBaseBuilder.consumer();
    }

    public enum AutoOffsetResetType {
        Latest("latest"), Earliest("earliest"), None("none");
        final String value;
        AutoOffsetResetType(String value) {
            this.value = value;
        }
    }
}
