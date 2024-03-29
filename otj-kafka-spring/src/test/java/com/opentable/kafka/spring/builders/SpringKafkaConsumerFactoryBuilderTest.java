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
package com.opentable.kafka.spring.builders;


import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.spring.AbstractTest;
import com.opentable.kafka.spring.builders.SpringKafkaConsumerFactoryBuilderTest.Config;

@ContextConfiguration(classes = Config.class)
@TestPropertySource(properties = {
        "ot.kafka.consumer.test.ack-mode=MANUAL",
        "ot.kafka.consumer.test.concurrency=1",
        "spring.main.allow-circular-references=true"
})
public class SpringKafkaConsumerFactoryBuilderTest extends AbstractTest {


    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate1;

    @Autowired
    private SettableListenableFuture<String> resultFuture1;


    @Test
    public void kafkaListenerTest() throws ExecutionException, InterruptedException, TimeoutException {
       kafkaTemplate1.send("topic-1", 1, "1").get();
       String data = resultFuture1.get(20, TimeUnit.SECONDS);
       assertEquals("1", data);
    }

    @Configuration
    @EnableKafka
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
                .build();
        }

        @Bean
        public SettableListenableFuture<String> resultFuture1() {
            return new SettableListenableFuture<>();
        }

        @KafkaListener(id = "foo", topics = "topic-1", containerFactory = "kafkaListenerContainerFactory", concurrency = "5")
        public void listen(String data) {
            resultFuture1().set(data);
        }

    }

}