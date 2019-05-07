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
package com.opentable.kafka.metrics;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import javax.inject.Inject;
import javax.management.MBeanServer;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.opentable.conservedheaders.ConservedHeader;
import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.InjectKafkaBuilderBean;
import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.builders.KafkaProducerBuilder;
import com.opentable.kafka.util.ReadWriteRule;
import com.opentable.metrics.DefaultMetricsConfiguration;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
})
public class MetricReporterTest {

    public static final String REQUEST_ID_KEY = ConservedHeader.REQUEST_ID.getLogName();

    @Rule
    public final ReadWriteRule rw = new ReadWriteRule();

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private EnvironmentProvider environmentProvider;

    public <K, V> Producer<K, V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        return new KafkaProducerBuilder<K,V>(rw.getEkb().baseProducerMap(), environmentProvider)
            .withClientId("producer-metrics-01")
            .withMetricRegistry(metricRegistry)
            .withSerializers(keySer, valueSer)
            .withProperty(ProducerConfig.LINGER_MS_CONFIG, "200")
            .build();
    }

    public void writeTestRecords(final int lo, final int hi) {
        try (Producer<String, String> producer = createProducer(StringSerializer.class, StringSerializer.class)) {
            for (int i = lo; i <= hi; ++i) {
                MDC.put(REQUEST_ID_KEY, UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(
                    rw.getTopicName(),
                    String.format("key-%d", i),
                    String.format("value-%d", i)));
            }
            producer.flush();
        }
    }

    public <K, V> Consumer<K, V> createConsumer(String groupId, Class<? extends Deserializer<K>> keySer, Class<? extends Deserializer<V>> valueSer) {
        return new KafkaConsumerBuilder<K,V>(rw.getEkb().baseConsumerMap(groupId), environmentProvider)
            .withClientId("consumer-metrics-01")
            .withMetricRegistry(metricRegistry)
            .withGroupId(groupId)
            .withMaxPollRecords(1)
            .withDeserializers(keySer, valueSer)
            .build();
   }

    public void readTestRecords(final int expect) {
        try (Consumer<String, String> consumer = createConsumer("test", StringDeserializer.class, StringDeserializer.class)) {
            consumer.subscribe(Collections.singleton(rw.getTopicName()));
            ConsumerRecords<String, String> records;
            while (true) {
                records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    break;
                }
            }
            Assertions.assertThat(records.count()).isEqualTo(expect);
            // Commit offsets.
            consumer.commitSync();
        }
    }


    @Test(timeout = 60_000)
    public void producerTest() {
        final int numTestRecords = 100;
        writeTestRecords(1, numTestRecords);
        rw.readTestRecords(numTestRecords);
    }

    @Test(timeout = 60_000)
    public void consumerTest() throws InterruptedException {
        final int numTestRecords = 100;
        writeTestRecords(1, numTestRecords);
        Consumer<String, String> consumer = createConsumer("test", StringDeserializer.class, StringDeserializer.class);
        consumer.subscribe(Collections.singleton(rw.getTopicName()));
        ConsumerRecords<String, String> records;
        while (true) {
            records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                break;
            }
        }
        consumer.commitSync();
        //                   kafka.consumer-metrics-01.test.0_topic-1.consumer-fetch-manager-metrics_records-lag-max
       waitForMetric(rw, "kafka.consumer-metrics-01.test.consumer-fetch-manager-metrics.records-lag-max.topic-1.0", (double)(numTestRecords - 1));
       waitForMetric(rw, "kafka.consumer-metrics-01.test.consumer-fetch-manager-metrics.records-lag-max", (double)(numTestRecords - 1));
       consumer.close();
    }

    private void waitForMetric(
        final ReadWriteRule rw,
        final String metricName,
        final Double value)
        throws InterruptedException {
        while (true) {
            Gauge gauge = metricRegistry.getGauges().get(metricName);
            if ((gauge != null) && (gauge.getValue().equals(value))) {
                break;
            }
            ReadWriteRule.loopSleep();
        }
    }

    @Configuration
    @InjectKafkaBuilderBean
    @Import(DefaultMetricsConfiguration.class)
    public static class Config {
        @Bean
        public MBeanServer getMBeanServer() {
            return ManagementFactory.getPlatformMBeanServer();
        }
        @Bean
        ServiceInfo serviceInfo(@Value("${info.component:test-service}") final String serviceType) {
            return () -> serviceType;
        }
    }

}
