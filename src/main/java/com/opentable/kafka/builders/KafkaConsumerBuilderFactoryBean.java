package com.opentable.kafka.builders;

import java.util.Optional;
import java.util.Properties;

import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.service.AppInfo;
import com.opentable.service.ServiceInfo;

public class KafkaConsumerBuilderFactoryBean<K,V> extends KafkaBaseBuilderFactoryBean {

    @Inject
    public KafkaConsumerBuilderFactoryBean(Optional<ServiceInfo> serviceInfo, AppInfo appInfo, ConfigurableEnvironment env, Optional<MetricRegistry> metricRegistry) {
        super(serviceInfo, appInfo, env, metricRegistry);
    }

    public KafkaConsumerBuilder<K,V> builder() {
        return builder(DEFAULT);
    }

    public KafkaConsumerBuilder<K,V> builder(String name) {
        final KafkaConsumerBuilder<K,V> res = new KafkaConsumerBuilder<>(
                getProperties(name,
                        getProperties( DEFAULT, new Properties())), appInfo);
        metricRegistry.ifPresent(mr -> res.withMetricRegistry(mr));
        serviceInfo.ifPresent(si -> res.withClientId(si.getName()));
        return res;
    }

}
