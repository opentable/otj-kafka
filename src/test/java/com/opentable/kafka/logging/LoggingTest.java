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

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.opentable.conservedheaders.ConservedHeader;
import com.opentable.kafka.util.ReadWriteRule;
import com.opentable.service.ServiceInfo;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles(profiles = "test")
@TestPropertySource(properties = {
    "info.component=test",
})
public class LoggingTest {

    public static final String REQUEST_ID_KEY = ConservedHeader.REQUEST_ID.getLogName();

    @Rule
    public final ReadWriteRule rw = new ReadWriteRule();

    public <K, V> KafkaProducer<K, V> createProducer(Class<? extends Serializer<K>> keySer, Class<? extends Serializer<V>> valueSer) {
        Properties props = rw.getEkb().baseProducerProperties();
        props.put("key.serializer", keySer.getName());
        props.put("value.serializer", valueSer.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getCanonicalName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "200");
        //put logger here
        return new KafkaProducer<>(props);
    }

    public void writeTestRecords(final int lo, final int hi) {
        MDC.put(REQUEST_ID_KEY, UUID.randomUUID().toString());
        try (KafkaProducer<String, String> producer = createProducer(StringSerializer.class, StringSerializer.class)) {
            for (int i = lo; i <= hi; ++i) {
                producer.send(new ProducerRecord<String, String>(
                    rw.getTopicName(),
                    String.format("key-%d", i),
                    String.format("value-%d", i)));
            }
            producer.flush();
        }
    }

    @Test(timeout = 60_000)
    public void test() {
        final int numTestRecords = 100;
        writeTestRecords(1, numTestRecords);
        rw.readTestRecords(numTestRecords);
    }

    @Configuration
    public static class Config {
        @Bean
        ServiceInfo serviceInfo(@Value("${info.component:test-service}") final String serviceType) {
            return () -> serviceType;
        }
    }
}
