package com.opentable.kafka.builders;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.opentable.kafka.builders.KafkaConsumerBuilder.AutoOffsetResetType;
import com.opentable.metrics.DefaultMetricsConfiguration;
import com.opentable.service.AppInfo;
import com.opentable.service.EnvInfo;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
})
public class KafkaConsumerBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerBuilderTest.class);

    @Autowired
    private KafkaBuilderFactoryBean builderFactoryBean;

    @Test
    public void builderTest() {
        KafkaConsumerBuilder<Integer, String> builder = builderFactoryBean.builder()
            .withBootstrapServers("localhost:8080")
            .withProp("blah", "blah")
            .withoutProp("blah")
            .withClientId("test-consomer-01")
            .consumer()
            .withProp("blah2", "blah2")
            .withoutProp("blah2")
            .withGroupId("test")
            .withDeserializers(IntegerDeserializer.class, StringDeserializer.class)
            .withOffsetReset(AutoOffsetResetType.latest);
        LOG.debug("Props: {}", builder.buildProps());
        KafkaConsumer<Integer, String> c = builder
            .build();
    }

    @Configuration
    @Import({
        AppInfo.class,
        EnvInfo.class,
        DefaultMetricsConfiguration.class,
        KafkaBuilderFactoryBean.class
    })
    public static class Config {
        @Bean
        ServiceInfo serviceInfo(@Value("${info.component:test-service}") final String serviceType) {
            return () -> serviceType;
        }
        @Bean
        public MBeanServer getMBeanServer() {
            return ManagementFactory.getPlatformMBeanServer();
        }
    }
}