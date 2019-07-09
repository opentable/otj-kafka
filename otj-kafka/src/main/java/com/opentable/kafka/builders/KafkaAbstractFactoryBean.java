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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.service.ServiceInfo;
import com.opentable.spring.PropertySourceUtil;

/**
 * Main spring entry point for building KafkaConsumer and KafkaProducer
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaAbstractFactoryBean {

    private static final String PREFIX = "ot.kafka.";
    private static final String DEFAULT = "default";
    private static final String NAME_CANNOT_BE_NULL = "Name cannot be null!";

    private final ConfigurableEnvironment env;
    private final Optional<MetricRegistry> metricRegistry;
    private final EnvironmentProvider environmentProvider;
    private final Optional<ServiceInfo> serviceInfo;
    private final String consumerPrefix;
    private final String producerPrefix;

    protected KafkaAbstractFactoryBean(
        final EnvironmentProvider environmentProvider,
        final ConfigurableEnvironment env,
        final Optional<ServiceInfo> serviceInfo,
        final Optional<MetricRegistry> metricRegistry
    ) {
        this.env = env;
        this.environmentProvider = environmentProvider;
        this.serviceInfo = serviceInfo;
        this.metricRegistry = metricRegistry;
        this.consumerPrefix = PREFIX + "consumer.";
        this.producerPrefix = PREFIX + "producer.";
    }

    // These take precedence. One minor flaw is list properties are not currently combined, these replace all
    protected Map<String, Object> getProperties(final String nameSpace, final String prefix) {
        return PropertySourceUtil.getProperties(env, prefix + nameSpace)
            .entrySet()
            .stream()
            .collect(Collectors.toMap(o -> (String) o.getKey(), Map.Entry::getValue));

    }

    // merge properties in, given the officially namespaced properties get precedence
    protected Map<String, Object> mergeProperties(final Map<String, Object> originalProperties, final String namespace, final String prefix) {
        final Map<String, Object> originalMap = new HashMap<>(originalProperties);
        final Map<String, Object> mergeMap = getProperties(namespace, prefix);
        originalMap.putAll(mergeMap);
        return originalMap;
    }

    protected Optional<MetricRegistry> getMetricRegistry() {
        return metricRegistry;
    }

    protected EnvironmentProvider getEnvironmentProvider() {
        return environmentProvider;
    }

    protected Optional<ServiceInfo> getServiceInfo() {
        return serviceInfo;
    }

    protected Map<String, Object> getMergedConsumerProperties(String name) {
        Objects.requireNonNull(name, NAME_CANNOT_BE_NULL);
        return mergeProperties(
            getProperties(DEFAULT, consumerPrefix),
            name, consumerPrefix
        );
    }

    protected Map<String, Object> getMergedProducerProperties(String name) {
        Objects.requireNonNull(name, NAME_CANNOT_BE_NULL);
        return mergeProperties(
            getProperties(DEFAULT, producerPrefix),
            name, producerPrefix
        );
    }

}
