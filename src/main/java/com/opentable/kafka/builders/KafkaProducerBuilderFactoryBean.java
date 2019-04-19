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
import java.util.Optional;

import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.service.AppInfo;
import com.opentable.service.ServiceInfo;

public class KafkaProducerBuilderFactoryBean extends KafkaBaseBuilderFactoryBean {

    @Inject
    public KafkaProducerBuilderFactoryBean(
                                           final AppInfo appInfo,
                                           final ConfigurableEnvironment env,
                                           final Optional<ServiceInfo> serviceInfo,
                                           final Optional<MetricRegistry> metricRegistry) {
        super(appInfo, env, serviceInfo, metricRegistry);
    }

    public <K,V> KafkaProducerBuilder<K,V> builder(Class<K> keyClass, Class<V> valueClass) {
        return builder(DEFAULT, keyClass, valueClass);
    }

    public <K,V> KafkaProducerBuilder<K,V> builder(String name, Class<K> keyClass, Class<V> valueClass) {
        final Map<String, Object> mergedSeedProperties = mergeProperties(
                getProperties(DEFAULT),
                name
        );
        final KafkaProducerBuilder<K,V> res = new KafkaProducerBuilder<>(mergedSeedProperties, appInfo);
        metricRegistry.ifPresent(res::withMetricRegistry);
        serviceInfo.ifPresent(si -> res.withClientId(si.getName()));
        return res;
    }

}
