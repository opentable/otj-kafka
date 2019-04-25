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

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import javax.inject.Inject;
import javax.management.MBeanServer;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import com.opentable.logging.otl.EdaMessageTraceV1;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
})
public class LoggingInterceptorsTest {

    @Rule
    public final ReadWriteRule rw = new ReadWriteRule();

    @Inject
    EnvironmentProvider environmentProvider;

    @Inject
    KafkaConsumerBuilderFactoryBean kafkaConsumerBuilderFactoryBean;

    @Inject
    KafkaProducerBuilderFactoryBean kafkaProducerBuilderFactoryBean;

    private CapturingLoggingUtils loggingUtils;

    public <K, V> Producer<K, V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        final KafkaProducerBuilder<K,V> builder =  createProducerBuilder(keySer, valueSer);
        return builder.build();
    }

    public <K, V> KafkaProducerBuilder<K, V> createProducerBuilder(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        final KafkaProducerBuilder<K,V> builder =  kafkaProducerBuilderFactoryBean.<K,V>builder("producer")
                .withSerializers(keySer, valueSer)
                .withProperty(ProducerConfig.LINGER_MS_CONFIG, "200")
                .disableMetrics();
        Map<String,Object> map = rw.getEkb().baseProducerMap();
        map.forEach(builder::withProperty);
        return builder;
    }

    public void writeTestRecords(final int lo, final int hi, Producer<String, String> producer) {
        //try (Producer<String, String> producer = createProducer(StringSerializer.class, StringSerializer.class)) {
         try {
            for (int i = lo; i <= hi; ++i) {
                List<Header> headers = new ArrayList<>();
                headers.add(new RecordHeader("myIndex", String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
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
        } finally {
             producer.close();
         }
    }

    public <K, V> Consumer<K, V> createConsumer(String groupId, Class<? extends Deserializer<K>> keySer, Class<? extends Deserializer<V>> valueSer) {
        return createConsumerBuilder(groupId, keySer, valueSer).build();
    }

    public <K, V>  KafkaConsumerBuilder<K,V> createConsumerBuilder(String groupId, Class<? extends Deserializer<K>> keySer, Class<? extends Deserializer<V>> valueSer) {
        Map<String,Object> props = rw.getEkb().baseConsumerMap(groupId);
        KafkaConsumerBuilder<K,V> builder = kafkaConsumerBuilderFactoryBean.<K,V>builder("consumer")
                .withDeserializers(keySer, valueSer).disableMetrics();
        props.forEach(builder::withProperty);
        return builder;
    }

    public ConsumerRecords<String, String> readTestRecords(final int expect, Consumer<String, String> consumer) {
        try {
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
        } finally {
            consumer.close();
        }
    }


    @Test(timeout = 60_000)
    public void producerTest() {
        final int numTestRecords = 100;
        writeTestRecords(1, numTestRecords, createProducer(StringSerializer.class, StringSerializer.class));
        rw.readTestRecords(numTestRecords);
    }

    @Test(timeout = 60_000)
    public void producerLoggingTest() {
        final int numTestRecords = 100;
        loggingUtils = new CapturingLoggingUtils(environmentProvider);
        Map<String, Object> props = ImmutableMap.of(
                LoggingInterceptorConfig.LOGGING_REF, loggingUtils,
                LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, Integer.MAX_VALUE, // effectively rate-unlimited
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName()

        );
        KafkaProducerBuilder<String, String> builder = createProducerBuilder(StringSerializer.class, StringSerializer.class)
                .disableLogging(); // this fools the builder so we can put a custom loggingutils instance.
        props.forEach(builder::withProperty);
        Producer<String, String> producer = builder.build();
        final UUID req = UUID.randomUUID();
        MDC.put(ConservedHeader.REQUEST_ID.getLogName(), req.toString());
        writeTestRecords(1, numTestRecords, producer);
        List<EdaMessageTraceV1> edaMessageTraceV1s = loggingUtils.getMessageTraceV1s();
        Assertions.assertThat(edaMessageTraceV1s).hasSize(numTestRecords);
        int index = 1;
        for (EdaMessageTraceV1 t : edaMessageTraceV1s) {
            commonLoggingAssertions(t, "kafka-producer", "producer-test", req, index, (o, o2) -> { });
            index++;
            // Unfortunately I note keysize, valuesize, timestamp, and partition aren't available since they are "post commit"
            // Until or unless we figure out how to correlate the postcommit, there's no good option. IIRC the kip doesn't guarantee anything about thread, so
            // it's pretty hard
        }
        rw.readTestRecords(numTestRecords);
    }

    private void commonLoggingAssertions(EdaMessageTraceV1 edaMessageTraceV1, String expectedLogName, String expectedClientId,
                                         UUID requestID, int index, BiConsumer<EdaMessageTraceV1, Integer> additionalAssertions) {
        Assertions.assertThat(edaMessageTraceV1.getKafkaClientId()).isEqualTo(expectedClientId);
        Assertions.assertThat(edaMessageTraceV1.getLogName()).isEqualTo(expectedLogName);
        Assertions.assertThat(edaMessageTraceV1.getRequestId()).isEqualTo(requestID);

        Assertions.assertThat(edaMessageTraceV1.getKafkaRecordKey()).isEqualTo("key-" + index);
        Assertions.assertThat(edaMessageTraceV1.getKafkaRecordValue()).isEqualTo("value-" + index);
        Assertions.assertThat(edaMessageTraceV1.getKafkaTopic()).isEqualTo("topic-1");
        Assertions.assertThat(edaMessageTraceV1.getKafkaVersion()).isNotNull();
        Assertions.assertThat(edaMessageTraceV1.getKafkaClientVersion()).isNotNull();
        Assertions.assertThat(edaMessageTraceV1.getKafkaClientName()).isEqualTo("otj-kafka");
        Assertions.assertThat(edaMessageTraceV1.getKafkaClientPlatform()).isEqualTo("java");

        Assertions.assertThat(edaMessageTraceV1.getKafkaClientPlaformVersion()).isNotNull();
        Assertions.assertThat(edaMessageTraceV1.getKafkaClientOs()).isNotNull();

        Assertions.assertThat(edaMessageTraceV1.getReferringHost()).isEqualTo("myHost");
        Assertions.assertThat(edaMessageTraceV1.getReferringService()).isEqualTo("myService");
        Assertions.assertThat(edaMessageTraceV1.getOtEnv()).isEqualTo("myEnv");
        Assertions.assertThat(edaMessageTraceV1.getOtEnvFlavor()).isEqualTo("myFlavor");
        Assertions.assertThat(edaMessageTraceV1.getReferringHost()).isEqualTo("myHost");

        additionalAssertions.accept(edaMessageTraceV1, index);
    }

    private String getHeaderValue(Headers headers, OTKafkaHeaders headerName) {
        return new String(headers.lastHeader(headerName.getKafkaName()).value(), StandardCharsets.UTF_8);
    }

    @Test(timeout = 60_000)
    public void consumerTest() {
        final int numTestRecords = 100;
        final UUID req = UUID.randomUUID();
        MDC.put(ConservedHeader.REQUEST_ID.getLogName(), req.toString());
        MDC.put(ConservedHeader.CORRELATION_ID.getLogName(), "foo");
        writeTestRecords(1, numTestRecords, createProducer(StringSerializer.class, StringSerializer.class));
        ConsumerRecords<String, String> r = readTestRecords(numTestRecords, createConsumer("test", StringDeserializer.class, StringDeserializer.class));
        // We demonstrate here that conserved headers + environment + a random added header all propagate.
        int expected = 1;
        for (ConsumerRecord<String, String> rec : r) {
            Headers headers = rec.headers();

            Assertions.assertThat(Integer.parseInt(new String(headers.lastHeader("myIndex").value(), StandardCharsets.UTF_8))).isEqualTo(expected);
            expected++;
            Assertions.assertThat(new String(headers.lastHeader(ConservedHeader.CORRELATION_ID.getLogName()).value(), StandardCharsets.UTF_8)).isEqualTo("foo");
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.REFERRING_SERVICE)).isEqualTo(environmentProvider.getReferringService());
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.ENV)).isEqualTo(environmentProvider.getEnvironment());
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.ENV_FLAVOR)).isEqualTo(environmentProvider.getEnvironmentFlavor());
            Assertions.assertThat(Integer.parseInt(getHeaderValue(headers, OTKafkaHeaders.REFERRING_INSTANCE_NO))).isEqualTo(environmentProvider.getReferringInstanceNumber());
            Assertions.assertThat(getHeaderValue(headers, OTKafkaHeaders.REQUEST_ID)).isEqualTo(req.toString());
        }
    }

    @Test(timeout = 60_000)
    public void consumerLoggingTest() {
        final int numTestRecords = 100;
        final UUID req = UUID.randomUUID();
        loggingUtils = new CapturingLoggingUtils(environmentProvider);
        MDC.put(ConservedHeader.REQUEST_ID.getLogName(), req.toString());
        writeTestRecords(1, numTestRecords, createProducer(StringSerializer.class, StringSerializer.class));
        Map<String, Object> props = ImmutableMap.of(
                LoggingInterceptorConfig.LOGGING_REF, loggingUtils,
                LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, Integer.MAX_VALUE, // effectively rate-unlimited
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName()

        );
        KafkaConsumerBuilder<String, String> builder = createConsumerBuilder("test", StringDeserializer.class, StringDeserializer.class)
                .disableLogging();
        props.forEach(builder::withProperty);
        ConsumerRecords<String, String> r = readTestRecords(numTestRecords, builder.build());
        int index = 1;
        List<EdaMessageTraceV1> edaMessageTraceV1List = loggingUtils.getMessageTraceV1s();
        for (EdaMessageTraceV1 t : edaMessageTraceV1List) {
            commonLoggingAssertions(t, "kafka-consumer", "consumer-test", req, index, (o, o2) -> { });
            index++;
            // Unfortunately I note keysize, valuesize, timestamp, and partition aren't available since they are "post commit"
            // Until or unless we figure out how to correlate the postcommit, there's no good option. IIRC the kip doesn't guarantee anything about thread, so
            // it's pretty hard
        }

    }

    @Configuration
    @InjectKafkaBuilderBeans
    public static class Config {
        @Bean
        ServiceInfo serviceInfo(@Value("${info.component:test-service}") final String serviceType) {
            return () -> serviceType;
        }

        @Bean
        public MBeanServer getMBeanServer() {
            return ManagementFactory.getPlatformMBeanServer();
        }

        @Bean(name="testEnvironmentProvider")
        @Primary
        public EnvironmentProvider environmentProvider() {
            return new SettableEnvironmentProvider("myService", "myHost", 5, "myEnv", "myFlavor");
        }
    }

    public static class CapturingLoggingUtils extends LoggingUtils {
        List<EdaMessageTraceV1> messageTraceV1s = new CopyOnWriteArrayList<>();
        public CapturingLoggingUtils(final EnvironmentProvider environmentProvider) {
            super(environmentProvider);
        }

        @Override
        protected void debugEvent(final EdaMessageTraceV1 edaMessageTraceV1) {
            messageTraceV1s.add(edaMessageTraceV1);
        }

        public List<EdaMessageTraceV1> getMessageTraceV1s() {
            return messageTraceV1s;
        }
    }
}
