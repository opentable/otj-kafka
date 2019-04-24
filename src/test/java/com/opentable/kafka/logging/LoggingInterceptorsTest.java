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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
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
import com.opentable.logging.CommonLogHolder;
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
                List<Header> headers = new ArrayList<>();
                headers.add(new RecordHeader("myIndex", String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
                MDC.put(REQUEST_ID_KEY, UUID.randomUUID().toString());
                producer.send(new ProducerRecord<>(
                        rw.getTopicName(),
                        null,
                        null,
                        String.format("key-%d", i),
                        String.format("value-%d", i),
                        headers
                        )
                );
            }
            producer.flush();
        }
    }

    public <K, V> Consumer<K, V> createConsumer(String groupId, Class<? extends Deserializer<K>> keySer, Class<? extends Deserializer<V>> valueSer) {
        Map<String,Object> props = rw.getEkb().baseConsumerMap(groupId);
        KafkaConsumerBuilder<K,V> builder = kafkaConsumerBuilderFactoryBean.<K,V>builder("consumer")
                .withDeserializers(keySer, valueSer).disableMetrics();
        props.forEach(builder::withProperty);
        KafkaConsumer<K,V> b =  builder.build();
        return b;
    }

    public ConsumerRecords<String, String> readTestRecords(final int expect) {
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
            return records;
        }
    }


    @Test(timeout = 60_000)
    public void producerTest() {
        final int numTestRecords = 100;
        writeTestRecords(1, numTestRecords);
        rw.readTestRecords(numTestRecords);
    }

    private String getHeaderValue(Headers headers, OTKafkaHeaders headerName) {
        return new String(headers.lastHeader(headerName.getKafkaName()).value(), StandardCharsets.UTF_8);
    }

    @Test(timeout = 60_000)
    public void consumerTest() {
        final int numTestRecords = 100;
        final UUID req = UUID.randomUUID();
        MDC.put(ConservedHeader.REQUEST_ID.getHeaderName(), req.toString());
        MDC.put(ConservedHeader.CORRELATION_ID.getHeaderName(), "foo");
        writeTestRecords(1, numTestRecords);
        ConsumerRecords<String, String> r = readTestRecords(numTestRecords);

        // We demonstrate here that conserved headers + environment + a random added header all propagate.
        int expected = 1;
        for (ConsumerRecord<String, String> rec : r) {
            Headers headers = rec.headers();

            Assertions.assertThat(Integer.parseInt(new String(headers.lastHeader("myIndex").value(), StandardCharsets.UTF_8))).isEqualTo(expected);
            expected++;
            Assertions.assertThat(new String(headers.lastHeader(ConservedHeader.CORRELATION_ID.getHeaderName()).value(), StandardCharsets.UTF_8)).isEqualTo("foo");
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.REFERRING_SERVICE)).isEqualTo(environmentProvider.getReferringService());
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.ENV)).isEqualTo(environmentProvider.getEnvironment());
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.ENV_FLAVOR)).isEqualTo(environmentProvider.getEnvironmentFlavor());
            Assertions.assertThat(Integer.parseInt(getHeaderValue(headers, OTKafkaHeaders.REFERRING_INSTANCE_NO))).isEqualTo(environmentProvider.getReferringInstanceNumber());
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.REQUEST_ID)).isEqualTo(req.toString());
            //TODO: Discuss logName vs headerName withh Scott.
        }
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
