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

import java.util.Optional;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.kafka.util.ClientIdGenerator;
import com.opentable.service.ServiceInfo;

/**
 * Main spring entry point for building KafkaConsumer and KafkaProducer
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KafkaBuilderFactoryBean extends KafkaAbstractFactoryBean {

    KafkaBuilderFactoryBean(EnvironmentProvider environmentProvider,
                            ConfigurableEnvironment env,
                            Optional<ServiceInfo> serviceInfo,
                            Optional<MetricRegistry> metricRegistry) {
        super(environmentProvider, env, serviceInfo, metricRegistry);
    }

    public KafkaConsumerBuilder<?, ?> consumerBuilder(String name) {
        final KafkaConsumerBuilder<?, ?> res = new KafkaConsumerBuilder<>(getMergedConsumerProperties(name), getEnvironmentProvider());
        getMetricRegistry().ifPresent(mr -> res.withMetricRegistry(mr, OtMetricsReporterConfig.DEFAULT_PREFIX + ".consumer." + name) );
        getServiceInfo().ifPresent(si -> res.withClientId(name + "-" + si.getName()  + "-" + ClientIdGenerator.getInstance().nextClientId()));
        return res;
    }

    public KafkaProducerBuilder<? , ?> producerBuilder(String name) {
        final KafkaProducerBuilder<? , ?> res = new KafkaProducerBuilder<>(getMergedProducerProperties(name), getEnvironmentProvider());
        getMetricRegistry().ifPresent(mr -> res.withMetricRegistry(mr, OtMetricsReporterConfig.DEFAULT_PREFIX + ".producer." + name) );
        getServiceInfo().ifPresent(si -> res.withClientId(name + "-" + si.getName() + "-" + ClientIdGenerator.getInstance().nextClientId()));
        return res;
    }

}
