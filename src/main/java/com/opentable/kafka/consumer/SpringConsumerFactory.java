package com.opentable.kafka.consumer;

import java.util.Optional;

import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.opentable.service.ServiceInfo;

public class SpringConsumerFactory<K,V> implements FactoryBean<Consumer<K,V>> {

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private Optional<ServiceInfo> serviceInfoOptional;


    @Override
    public Consumer<K, V> getObject() throws Exception {
        ConsumerBuilder<K,V> builder =  new ConsumerBuilder<K,V>()
                .withMetricRegistry(metricRegistry);
        serviceInfoOptional.ifPresent(s -> builder.setApplicationName(s.getName()));
        // And many more.. the builder here mirror the one in consumer builder, but
        // can inject many niceties as defaults...
        return builder.build();
    }

    @Override
    public Class<?> getObjectType() {
        return Consumer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
