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
package com.opentable.kafka.logging;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.opentable.conservedheaders.ConservedHeader;
import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.InjectKafkaBuilderBeans;
import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.builders.KafkaConsumerBuilderFactoryBean;
import com.opentable.kafka.builders.KafkaProducerBuilder;
import com.opentable.kafka.builders.KafkaProducerBuilderFactoryBean;
import com.opentable.kafka.builders.SettableEnvironmentProvider;
import com.opentable.kafka.util.ReadWriteRule;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
})
public class LoggingInterceptorsTest {

    public static final String REQUEST_ID_KEY = ConservedHeader.REQUEST_ID.getLogName();

    @Rule
    public final ReadWriteRule rw = new ReadWriteRule();

    @Inject
    EnvironmentProvider environmentProvider;

    @Inject
    KafkaConsumerBuilderFactoryBean kafkaConsumerBuilderFactoryBean;

    @Inject
    KafkaProducerBuilderFactoryBean kafkaProducerBuilderFactoryBean;

    public <K, V> Producer<K, V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {

        KafkaProducerBuilder<K,V> builder =  kafkaProducerBuilderFactoryBean.<K,V>builder("producer")
                .withSerializers(keySer, valueSer)
                .withProperty(ProducerConfig.LINGER_MS_CONFIG, "200")
                .disableMetrics();
        Map<String,Object> map = rw.getEkb().baseProducerMap();
        map.forEach(builder::withProperty);
        return builder.build();

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
        /*
        Properties props = rw.getEkb().baseConsumerProperties(groupId);
        props.put("key.deserializer", keySer.getName());
        props.put("value.deserializer", valueSer.getName());
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getCanonicalName());
        props.put(LoggingInterceptorConfig.LOGGING_REF, new LoggingUtils(environmentProvider));
        //put logger here

        return new KafkaConsumer<>(props)
         */

        Map<String,Object> props = rw.getEkb().baseConsumerMap(groupId);
        KafkaConsumerBuilder<K,V> builder = kafkaConsumerBuilderFactoryBean.<K,V>builder("consumer")
                .withDeserializers(keySer, valueSer).disableMetrics();
        props.forEach(builder::withProperty);
        KafkaConsumer<K,V> b =  builder.build();
        return b;
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
    public void consumerTest() {
        final int numTestRecords = 100;
        writeTestRecords(1, numTestRecords);
        readTestRecords(numTestRecords);
    }

    @Configuration
    @InjectKafkaBuilderBeans
    public static class Config {
        @Bean
        ServiceInfo serviceInfo(@Value("${info.component:test-service}") final String serviceType) {
            return () -> serviceType;
        }

        @Bean(name="testEnvironmentProvider")
        @Primary
        public EnvironmentProvider environmentProvider() {
            return new SettableEnvironmentProvider("myService", "myHost", 5, "myEnv", "myFlavor");
        }
    }
}
