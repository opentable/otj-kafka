package com.opentable.kafka.builders;

import java.util.Optional;

import com.codahale.metrics.MetricRegistry;

import org.springframework.core.env.ConfigurableEnvironment;

import com.opentable.service.ServiceInfo;
/**
 * Use if you absolutely positively don't have a Spring Environment for your tests.
 * Generally this means a problem in your tests, but this can be handy
 *
 * Methods below need "ConfigurableEnvironment" - If you import spring-tests, you may use MockEnvironment
 */
public class MockKafkaBuilderFactoryBean extends KafkaBuilderFactoryBean {
    /**
     * Most complex but general constructor
     * @param environmentProvider Use SettableEnvironmentProvider for mocks. It's part of otj-kafka main package
     * @param env Use MockEnvironment for Mocks, it's part of spring-tests
     * @param serviceInfo Mock this
     * @param metricRegistry Most of the time pass, Optional.empty()
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public MockKafkaBuilderFactoryBean(final EnvironmentProvider environmentProvider, final ConfigurableEnvironment env, final Optional<ServiceInfo> serviceInfo, final Optional<MetricRegistry> metricRegistry) {
        super(environmentProvider, env, serviceInfo, metricRegistry);
    }

    /**
     * Return with  MockEnvironment injected, and no Metrics
     * @param environment Use MockEnvironment from spring-tests
     * @param environmentProvider Use SettableEnvironmentProvider for mocks. It's part of otj-kafka main package
     * @param serviceInfo Mock this - it's just a lambda
     * @return your bean
     */
    public static MockKafkaBuilderFactoryBean mock(final ConfigurableEnvironment environment, final EnvironmentProvider environmentProvider, ServiceInfo serviceInfo) {
        return new MockKafkaBuilderFactoryBean(environmentProvider, environment, Optional.ofNullable(serviceInfo), Optional.empty());
    }

    /**
     * If you are ok with all fake values, no metrics etc, this is the quick and dirty way
     *  @param environment Use MockEnvironment from spring-tests
     * @return your bean
     */
    public static MockKafkaBuilderFactoryBean mock(ConfigurableEnvironment environment) {
        return mock(environment, new SettableEnvironmentProvider("mock-service",
                "mock-host", 1,"mock-env", null), () -> "nock-service");
    }
}
