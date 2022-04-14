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
package com.opentable.kafka.spring.features;


import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.ContextConfiguration;

import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.spring.AbstractTest;
import com.opentable.kafka.spring.builders.KafkaFactoryBuilderFactoryBean;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = BatchListenerTest.Config.class)
@TestPropertySource(properties = { "spring.main.allow-circular-references=true" })
public class BatchListenerTest extends AbstractTest {


    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate1;


    @Autowired
    private KafkaListenerEndpointRegistry registry;


    private static CountDownLatch countDownLatch;
    private static final List<String> data = new CopyOnWriteArrayList<>();

    @Before
    public void before() {
        data.clear();
        countDownLatch = new CountDownLatch(5);
    }

    @Configuration
    public static class Config {

        @Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        protected String brokerAddresses;


        @Autowired
        private KafkaFactoryBuilderFactoryBean kafkaFactoryBuilderFactoryBean;

        @Bean
        public ProducerFactory<Integer, String> producerFactory1() {
            return kafkaFactoryBuilderFactoryBean.producerFactoryBuilder("test")
                .withBootstrapServer(brokerAddresses)
                .withSerializers(IntegerSerializer.class, StringSerializer.class)
                .build();
        }

        @Bean
        public KafkaTemplate<Integer, String> kafkaTemplate1() {
            return new KafkaTemplate<>(producerFactory1());
        }

        @Bean
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory() {
            return kafkaFactoryBuilderFactoryBean.consumerFactoryBuilder("test")
                .withBootstrapServer(brokerAddresses)
                .withDeserializers(IntegerDeserializer.class, StringDeserializer.class)
                .withAutoOffsetReset(KafkaConsumerBuilder.AutoOffsetResetType.Earliest)
                .withBatchListener(true)
                .withAckMode(AckMode.BATCH)
                .build();
        }

        @KafkaListener(id = "foo", topics = "topic-1", containerFactory = "kafkaListenerContainerFactory", concurrency = "1")
        public void listen(List<String> data) {
            System.err.println("Called " +data);
            BatchListenerTest.data.addAll(data);
            data.forEach(t -> countDownLatch.countDown());
        }

    }

    @Test(timeout = 60000)
    public void kafkaBatchListenerTest() throws ExecutionException, InterruptedException, TimeoutException {
        registry.getListenerContainer("foo").pause();
        Thread.sleep(5000);
        kafkaTemplate1.send("topic-1", 1, "1").get();
        kafkaTemplate1.send("topic-1", 2, "1").get();
        kafkaTemplate1.send("topic-1", 3, "1").get();
        kafkaTemplate1.send("topic-1", 4, "1").get();
        kafkaTemplate1.send("topic-1", 4, "1").get();
        registry.getListenerContainer("foo").resume();
        //Dmitry this test was flaky, at least on the new PR
        //The batching size in Kafka doesn't reliably give you 5, I've redone to handle this
        countDownLatch.await();
        assertEquals(5, data.size());
    }

}
