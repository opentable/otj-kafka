package com.opentable.kafka.builders;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.service.AppInfo;
import com.opentable.service.ServiceInfo;
import com.opentable.spring.PropertySourceUtil;

/**
 * A base class with the common fields and methods
 */
public class KafkaBaseBuilderFactoryBean {

    protected static final String PREFIX = "ot.kafka.";
    protected static final String DEFAULT = "default";

    protected final ConfigurableEnvironment env;
    protected final Optional<MetricRegistry> metricRegistry;
    protected final AppInfo appInfo;
    protected final Optional<ServiceInfo> serviceInfo;

    public KafkaBaseBuilderFactoryBean(Optional<ServiceInfo> serviceInfo, AppInfo appInfo, ConfigurableEnvironment env, Optional<MetricRegistry> metricRegistry) {
        this.env = env;
        this.appInfo = appInfo;
        this.serviceInfo = serviceInfo;
        this.metricRegistry = metricRegistry;
    }

    // One flaw in my approach is that validation won't work correctly against these. We can talk about this

    protected Properties getProperties(final String nameSpace, final Properties res) {
        res.putAll(
                PropertySourceUtil.getProperties(env, PREFIX + nameSpace)
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(o -> (String)o.getKey(), Map.Entry::getValue))
        );
        return res;
    }

}
