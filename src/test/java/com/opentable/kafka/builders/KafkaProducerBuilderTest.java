package com.opentable.kafka.builders;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import com.opentable.kafka.builders.KafkaProducerBuilder.AckType;
import com.opentable.metrics.DefaultMetricsConfiguration;
import com.opentable.service.AppInfo;
import com.opentable.service.EnvInfo;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
    "ot.kafka.producer.linger.ms=10"
})
public class KafkaProducerBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerBuilderTest.class);

    @Autowired
    private KafkaBuilderFactoryBean builderFactoryBean;

    @Test
    public void builderTest() {
        KafkaProducerBuilder<Integer, String> builder = builderFactoryBean.builder("producer")
            .withBootstrapServers("localhost:8080")
            .withProp("blah", "blah")
            .withoutProp("blah")
            .producer()
            .withProp("blah2", "blah2")
            .withoutProp("blah2")
            .withAcks(AckType.none)
            .withRetries(5)
            .withSerializers(IntegerSerializer.class, StringSerializer.class);
        LOG.debug("Props: {}", builder.buildProps());
        KafkaProducer<Integer, String> p = builder
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