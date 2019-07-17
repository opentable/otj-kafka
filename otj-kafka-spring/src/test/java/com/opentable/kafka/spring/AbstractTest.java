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
package com.opentable.kafka.spring;


import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.opentable.kafka.spring.builders.InjectSpringKafkaBuilderBean;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = AbstractTest.Config.class)
@ActiveProfiles(profiles = "test")
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"topic-1", "topic-2"})
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
    @InjectSpringKafkaBuilderBean
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
