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
import java.util.Optional;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.kafka.util.ClientIdGenerator;
import com.opentable.service.ServiceInfo;
import com.opentable.spring.PropertySourceUtil;

/**
 * Main spring entry point for building KafkaConsumer and KafkaProducer
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaBuilderFactoryBean {

    protected static final String PREFIX = "ot.kafka.";
    static final String DEFAULT = "default";

    private final ConfigurableEnvironment env;
    final Optional<MetricRegistry> metricRegistry;
    final EnvironmentProvider environmentProvider;
    final Optional<ServiceInfo> serviceInfo;
    private final String consumerPrefix;
    private final String producerPrefix;

    KafkaBuilderFactoryBean(
            final EnvironmentProvider environmentProvider,
            final ConfigurableEnvironment env,
            final Optional<ServiceInfo> serviceInfo,
            final Optional<MetricRegistry> metricRegistry) {
        this.env = env;
        this.environmentProvider = environmentProvider;
        this.serviceInfo = serviceInfo;
        this.metricRegistry = metricRegistry;
        this.consumerPrefix = PREFIX + "consumer.";
        this.producerPrefix = PREFIX + "producer.";
    }

    // These take precedence. One minor flaw is list properties are not currently combined, these replace all
    Map<String, Object> getProperties(final String nameSpace, final String prefix) {
        return PropertySourceUtil.getProperties(env, prefix + nameSpace)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(o -> (String) o.getKey(), Map.Entry::getValue));

    }

    // merge properties in, given the officially namespaced properties get precedence
    Map<String, Object> mergeProperties(final Map<String, Object> originalProperties, final String namespace, final String prefix) {
        final Map<String, Object> originalMap = new HashMap<>(originalProperties);
        final Map<String, Object> mergeMap = getProperties(namespace, prefix);
        originalMap.putAll(mergeMap);
        return originalMap;
    }


    public KafkaConsumerBuilder<?, ?> consumerBuilder() {
        return consumerBuilder(DEFAULT);
    }

    public KafkaConsumerBuilder<?, ?> consumerBuilder(String name) {
        final Map<String, Object> mergedSeedProperties = mergeProperties(
            getProperties(DEFAULT, consumerPrefix),
            name, consumerPrefix
        );
        final KafkaConsumerBuilder<?, ?> res = new KafkaConsumerBuilder<>(mergedSeedProperties, environmentProvider);
        metricRegistry.ifPresent(res::withMetricRegistry);
        serviceInfo.ifPresent(si -> res.withClientId(name + "-" + si.getName()  + "-" + ClientIdGenerator.getInstance().nextClientId()));
        return res;
    }

    public KafkaProducerBuilder<?, ?> producerBuilder() {
        return producerBuilder(DEFAULT);
    }

    public KafkaProducerBuilder<? , ?> producerBuilder(String name) {
        final Map<String, Object> mergedSeedProperties = mergeProperties(
            getProperties(DEFAULT, this.producerPrefix),
            name, producerPrefix
        );
        final KafkaProducerBuilder<? , ?> res = new KafkaProducerBuilder<>(mergedSeedProperties, environmentProvider);
        metricRegistry.ifPresent(res::withMetricRegistry);
        serviceInfo.ifPresent(si -> res.withClientId(name + "-" + si.getName() + "-" + ClientIdGenerator.getInstance().nextClientId()));
        return res;
    }


}
