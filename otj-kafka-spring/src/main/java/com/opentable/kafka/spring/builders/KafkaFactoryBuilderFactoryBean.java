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
package com.opentable.kafka.spring.builders;

import java.util.Optional;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaAbstractFactoryBean;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.kafka.util.ClientIdGenerator;
import com.opentable.service.ServiceInfo;

/**
 * Main spring entry point for building ConsumerFactory and ProducerFactory
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaFactoryBuilderFactoryBean extends KafkaAbstractFactoryBean {

    private final Optional<ObjectMapper> objectMapper;

    KafkaFactoryBuilderFactoryBean(EnvironmentProvider environmentProvider,
                                   ConfigurableEnvironment env,
                                   Optional<ServiceInfo> serviceInfo,
                                   Optional<MetricRegistry> metricRegistry,
                                   Optional<ObjectMapper> objectMapper
    ) {
        super(environmentProvider, env, serviceInfo, metricRegistry);
        this.objectMapper = objectMapper;
    }


    public SpringKafkaProducerFactoryBuilder<?, ?> producerFactoryBuilder(String name) {
        final SpringKafkaProducerFactoryBuilder<?, ?> res = new SpringKafkaProducerFactoryBuilder<>(getMergedProducerProperties(name), getEnvironmentProvider());
        getMetricRegistry().ifPresent(mr -> res.withMetricRegistry(mr, OtMetricsReporterConfig.DEFAULT_PREFIX + ".producer." + name + ".${metric-reporter-id}"));
        getServiceInfo().ifPresent(si -> res.withClientId(name + "-" + si.getName() + "-" + ClientIdGenerator.getInstance().nextClientId()));
        return res;
    }

    public SpringKafkaConsumerFactoryBuilder<?, ?> consumerFactoryBuilder(String name) {
        final SpringKafkaConsumerFactoryBuilder<?, ?> res = new SpringKafkaConsumerFactoryBuilder<>(getMergedConsumerProperties(name), getEnvironmentProvider(), objectMapper);
        getMetricRegistry().ifPresent(mr -> res.withMetricRegistry(mr, OtMetricsReporterConfig.DEFAULT_PREFIX + ".consumer." + name + ".${metric-reporter-id}"));
        getServiceInfo().ifPresent(si -> res.withClientId(name + "-" + si.getName() + "-" + ClientIdGenerator.getInstance().nextClientId()));
        return res;
    }

}
