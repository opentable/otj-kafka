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
import java.time.Duration;
import java.util.Map;

import javax.inject.Inject;
import javax.management.MBeanServer;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import com.opentable.kafka.builders.KafkaProducerBuilder.AckType;
import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingProducerInterceptor;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.metrics.DefaultMetricsConfiguration;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
    "ot.kafka.producer.default.linger.ms=10",
    "ot.kafka.producer.testme.linger.ms=20",
    "ot.kafka.producer.funky.interceptor.classes=com.opentable.kafka.builders.KafkaProducerBuilderTest$Foo"
})
public class KafkaProducerBuilderTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerBuilderTest.class);

    @Inject
    private KafkaBuilderFactoryBean builderFactoryBean;

    @Inject
    private MetricRegistry metricRegistry;

    @Test
    public void builderTest() {
        KafkaProducerBuilder<Integer, String> builder = getBuilder("testme");
        Producer<Integer, String> p = builder
                .build();
        Map<String, Object> finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties).isNotEmpty();
        assertThat(finalProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:8080");
        assertThat(finalProperties).doesNotContainKeys("blah");
        assertThat(finalProperties.get(CommonClientConfigs.CLIENT_ID_CONFIG)).isEqualTo("test-producer-01");
        assertThat(finalProperties.get("blah2")).isEqualTo("blah2");
        assertThat(finalProperties.get(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo("30000");
        assertThat(finalProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)).isEqualTo(SecurityProtocol.PLAINTEXT.name);
        assertThat(finalProperties.get( ProducerConfig.ACKS_CONFIG)).isEqualTo(AckType.none.value);
        assertThat(finalProperties.get(CommonClientConfigs.RETRIES_CONFIG)).isEqualTo(5);
        assertThat(finalProperties).doesNotContainKeys( CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
        // metrics, logging, overriding properties
        assertThat(finalProperties.get(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG)).isEqualTo(OtMetricsReporter.class.getName());
        assertThat(finalProperties.get(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG)).isSameAs(metricRegistry);
        assertThat(finalProperties.get(LoggingInterceptorConfig.LOGGING_ENV_REF)).isInstanceOf(EnvironmentProvider.class);
        assertThat(finalProperties.get(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG)).isEqualTo(3);
        assertThat(finalProperties.get("linger.ms")).isEqualTo("20");
        assertThat(finalProperties.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)).isEqualTo(LoggingProducerInterceptor.class.getName());
        assertThat(finalProperties.get(ProducerConfig.PARTITIONER_CLASS_CONFIG)).isEqualTo(DefaultPartitioner.class.getName());
        assertThat(finalProperties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)).isEqualTo(5);
        assertThat(finalProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class.getName());
        assertThat(finalProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class.getName());
        p.close();

    }

    @Test
    public void withoutMetricsAndLogging() {
        KafkaProducerBuilder<Integer, String> builder = getBuilder("producer");
        Producer<Integer, String> p = builder.disableLogging().disableMetrics()
                .build();
        p.close();
        Map<String, Object> finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties).doesNotContainKeys(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG,
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                LoggingInterceptorConfig.LOGGING_ENV_REF, LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG );
    }

    @Test
    public void customInterceptor() {
        KafkaProducerBuilder<Integer, String> builder = getBuilder("producer");
        Producer<Integer, String> p = builder
                .withInterceptor(Foo.class)
                .build();
        p.close();
        Map<String, Object> finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)).isEqualTo(Foo.class.getName() + "," + LoggingProducerInterceptor.class.getName());

        // Also works with a property
        builder = getBuilder("funky");
         p = builder
                .build();
        p.close();
        finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)).isEqualTo(LoggingProducerInterceptor.class.getName() + "," + Foo.class.getName());


        // Also works with no logging
        builder = getBuilder("funky");
        p = builder.disableLogging()
                .build();
        p.close();
        finalProperties = builder.getKafkaBaseBuilder().getFinalProperties();
        assertThat(finalProperties.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG)).isEqualTo(Foo.class.getName());

    }

    private KafkaProducerBuilder<Integer, String> getBuilder(String name) {
        return builderFactoryBean.producerBuilder(name)
                    .withBootstrapServer("localhost:8080")
                    .withProperty("blah", "blah")
                    .withProperty("linger.ms", "99") // this will be overwritten
                    .removeProperty("blah")
                    .withClientId("test-producer-01")
                    .withProperty("blah2", "blah2")
                    .withAcks(AckType.none)
                    .withRetries(5)
                    .withMaxInFlightRequests(5)
                    .withRequestTimeoutMs(Duration.ofSeconds(30))
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSerializers(IntegerSerializer.class, StringSerializer.class)
                    .withSamplingRatePer10Seconds(3);
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

    public static class Foo implements ProducerInterceptor<Integer, String> {

        @Override
        public ProducerRecord<Integer, String> onSend(final ProducerRecord<Integer, String> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(final RecordMetadata metadata, final Exception exception) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(final Map<String, ?> configs) {

        }
    }
}