package com.opentable.kafka.builders;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.stereotype.Component;

import com.opentable.service.AppInfo;
import com.opentable.spring.PropertySourceUtil;

@Component
public class KafkaBuilderFactoryBean {

    private static final String PREFIX = "ot.kafka.";
    private static final String DEFAULT = "default";

    private final ConfigurableEnvironment env;
    private final Optional<MetricRegistry> metricRegistry;
    private final AppInfo appInfo;

    @Inject
    public KafkaBuilderFactoryBean(AppInfo appInfo, ConfigurableEnvironment env, Optional<MetricRegistry> metricRegistry) {
        this.env = env;
        this.appInfo = appInfo;
        this.metricRegistry = metricRegistry;
    }

    public KafkaBuilder builder() {
        return builder(DEFAULT);
    }

    public KafkaBuilder builder(String name) {
        final KafkaBuilder res = KafkaBuilder.builder(
                getProperties(name, getProperties(DEFAULT, new Properties())), appInfo);
        return metricRegistry.map(res::withMetricReporter).orElse(res);
    }

    private Properties getProperties(final String nameSpace, final Properties res) {
        res.putAll(
            PropertySourceUtil.getProperties(env, PREFIX + nameSpace)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(o -> (String)o.getKey(), Entry::getValue))
        );
        return res;
    }

}
