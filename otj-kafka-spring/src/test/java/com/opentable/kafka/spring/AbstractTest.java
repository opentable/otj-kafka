package com.opentable.kafka.spring;


import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.opentable.kafka.builders.InjectKafkaBuilderBean;
import com.opentable.kafka.spring.builders.SpringKafkaConfiguration;
import com.opentable.metrics.DefaultMetricsConfiguration;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = AbstractTest.Config.class)
@ActiveProfiles(profiles = "test")
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "topic-1"})
@TestPropertySource(properties = {
    "kafka.enabled=true",
    "kafka.consumer.enabled=true",
    "kafka.producer.enabled=true",
    "kafka.brokers=${spring.embedded.kafka.brokers}",
    "kafka.consumer.group.id=testGroup",
    "logging.level.org.apache.zookeeper=ERROR",
    "logging.level.kafka=ERROR",
    "logging.level.org.I0Itec.zkclient=ERROR"
})
public abstract class AbstractTest {

    @Autowired
    protected ConfigurableEnvironment env;

    @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
    protected String brokerAddresses;

    @Configuration
    @InjectKafkaBuilderBean
    @Import({
        DefaultMetricsConfiguration.class,
        SpringKafkaConfiguration.class
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
