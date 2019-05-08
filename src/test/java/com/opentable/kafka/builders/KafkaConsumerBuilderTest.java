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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.inject.Inject;
import javax.management.MBeanServer;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
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
import com.opentable.kafka.logging.LoggingConsumerInterceptor;
import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.metrics.DefaultMetricsConfiguration;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
        "info.component=test",
        "ot.kafka.consumer.testme.check.crcs=false"
})
public class KafkaConsumerBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerBuilderTest.class);

    @Inject
    private KafkaBuilderFactoryBean builderFactoryBean;

    @Inject
    private MetricRegistry metricRegistry;

    @Test
    public void builderTest() {
        KafkaConsumerBuilder<Integer, String> builder = getBuilder();
        Consumer<Integer, String> c = builder
                .build();
        Map<String, Object> finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties).isNotEmpty();
        assertThat(finalProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:8080");
        assertThat(finalProperties).doesNotContainKeys("blah");
        assertThat(finalProperties.get(CommonClientConfigs.CLIENT_ID_CONFIG)).isEqualTo("test-consumer-01");
        assertThat(finalProperties.get("blah2")).isEqualTo("blah2");
        assertThat(finalProperties.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("test");
        assertThat(finalProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerDeserializer.class.getName());
        assertThat(finalProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class.getName());
        assertThat(finalProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo(AutoOffsetResetType.Latest.value);
        assertThat(finalProperties.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)).isEqualTo(RangeAssignor.class.getName());
        assertThat(finalProperties).doesNotContainKeys(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        assertThat(finalProperties).doesNotContainKeys(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        // metrics, logging, overriding properties
        assertThat(finalProperties.get(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG)).isEqualTo(OtMetricsReporter.class.getName());
        assertThat(finalProperties.get(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG)).isSameAs(metricRegistry);
        assertThat(finalProperties.get(LoggingInterceptorConfig.LOGGING_ENV_REF)).isInstanceOf(EnvironmentProvider.class);
        assertThat(finalProperties.get(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG)).isEqualTo(3);
        assertThat(finalProperties.get(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)).isEqualTo(LoggingConsumerInterceptor.class.getName());
        assertThat(finalProperties.get("check.crcs")).isEqualTo("false");
        c.close();
    }

    @Test
    public void withoutLoggingOrMetrics() {
        KafkaConsumerBuilder<Integer, String> builder = getBuilder().disableLogging().disableMetrics();
        Consumer<Integer, String> c = builder
                .build();
        Map<String, Object> finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties).doesNotContainKeys(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG,
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                LoggingInterceptorConfig.LOGGING_ENV_REF, LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG );
        c.close();
    }



    private KafkaConsumerBuilder<Integer, String> getBuilder() {
        return builderFactoryBean.consumerBuilder("testme")
                .withBootstrapServer("localhost:8080")
                .withProperty("blah", "blah")
                .removeProperty("blah")
                .withClientId("test-consumer-01")
                .withProperty("blah2", "blah2")
                .withGroupId("test")
                .withDeserializers(IntegerDeserializer.class, StringDeserializer.class)
                .withAutoOffsetReset(AutoOffsetResetType.Latest)
                .withSamplingRatePer10Seconds(3);
    }

    @Configuration
    @InjectKafkaBuilderBean
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