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
import java.util.Properties;

import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.service.AppInfo;
import com.opentable.service.ServiceInfo;

public class KafkaProducerBuilderFactoryBean<K,V> extends KafkaBaseBuilderFactoryBean {

    @Inject
    public KafkaProducerBuilderFactoryBean(Optional<ServiceInfo> serviceInfo, AppInfo appInfo, ConfigurableEnvironment env, Optional<MetricRegistry> metricRegistry) {
        super(serviceInfo, appInfo, env, metricRegistry);
    }

    public KafkaProducerBuilder<K,V> builder() {
        return builder(DEFAULT);
    }

    public KafkaProducerBuilder<K,V> builder(String name) {
        final KafkaProducerBuilder<K,V> res = new KafkaProducerBuilder<>(
                getProperties(name,
                        getProperties( DEFAULT, new Properties())), appInfo);
        metricRegistry.ifPresent(mr -> res.withMetricRegistry(mr));
        serviceInfo.ifPresent(si -> res.withClientId(si.getName()));
        return res;
    }

}
