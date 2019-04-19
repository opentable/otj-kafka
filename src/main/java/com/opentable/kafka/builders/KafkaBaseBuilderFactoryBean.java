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

import com.opentable.service.AppInfo;
import com.opentable.service.ServiceInfo;
import com.opentable.spring.PropertySourceUtil;

/**
 * A base class with the common fields and methods
 */
class KafkaBaseBuilderFactoryBean {

    private static final String PREFIX = "ot.kafka.";
    static final String DEFAULT = "default";

    private final ConfigurableEnvironment env;
    protected final Optional<MetricRegistry> metricRegistry;
    final AppInfo appInfo;
    final Optional<ServiceInfo> serviceInfo;

    KafkaBaseBuilderFactoryBean(
            final AppInfo appInfo,
            final ConfigurableEnvironment env,
            final Optional<ServiceInfo> serviceInfo,
            final Optional<MetricRegistry> metricRegistry) {
        this.env = env;
        this.appInfo = appInfo;
        this.serviceInfo = serviceInfo;
        this.metricRegistry = metricRegistry;
    }

    // These take precedence. One minor flaw is list properties are not currently combined, these replace all
    Map<String, Object> getProperties(final String nameSpace) {
        return PropertySourceUtil.getProperties(env, PREFIX + nameSpace)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(o -> (String) o.getKey(), Map.Entry::getValue));

    }

    Map<String, Object> mergeProperties(final Map<String, Object> originalProperties, final String namespace) {
        final Map<String, Object> originalMap = new HashMap<>(originalProperties);
        final Map<String, Object> mergeMap = getProperties(namespace);
        originalMap.putAll(mergeMap);
        return originalMap;
    }

}
