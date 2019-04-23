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

import java.lang.management.ManagementFactory;

import javax.inject.Inject;
import javax.management.MBeanServer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
        "info.component=test",
        "ot.kafka.consumer.check.crcs=false"
})
public class KafkaConsumerBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerBuilderTest.class);

    @Inject
    private KafkaConsumerBuilderFactoryBean builderFactoryBean;

    @Test
    public void builderTest() {
        KafkaConsumerBuilder<Integer, String> builder = builderFactoryBean.builder("consumer", Integer.class, String.class)
                .withBootstrapServer("localhost:8080")
                .withProperty("blah", "blah")
                .removeProperty("blah")
                .withClientId("test-consomer-01")
                .withProperty("blah2", "blah2")
                .removeProperty("blah2")
                .withGroupId("test")
                .withDeserializers(IntegerDeserializer.class, StringDeserializer.class)
                .withAutoOffsetReset(AutoOffsetResetType.Latest)
                .withLoggingSampleRate(3);
        KafkaConsumer<Integer, String> c = builder
                .build();
    }

    @Configuration
    @InjectKafkaBuilderBeans
    @Import({
            DefaultMetricsConfiguration.class,
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