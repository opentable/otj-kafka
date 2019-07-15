package com.opentable.kafka.spring.features;


import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.spring.AbstractTest;
import com.opentable.kafka.spring.builders.KafkaFactoryBuilderFactoryBean;

@ContextConfiguration(classes = BatchListenerTest.Config.class)
public class BatchListenerTest extends AbstractTest {


    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate1;

    @Autowired
    @Qualifier("resultFuture1")
    private SettableListenableFuture<List<String>> resultFuture1;

    @Autowired
    private KafkaListenerEndpointRegistry registry;



    @Configuration
    public static class Config {

        @Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
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

        @Bean
        @Qualifier("resultFuture1")
        public SettableListenableFuture<List<String>> resultFuture1() {
            return new SettableListenableFuture<>();
        }

        @KafkaListener(id = "foo", topics = "topic-1", containerFactory = "kafkaListenerContainerFactory", concurrency = "1")
        public void listen(List<String> data) {
            resultFuture1().set(data);
        }

    }

    @Test
    public void kafkaBatchListenerTest() throws ExecutionException, InterruptedException, TimeoutException {
        registry.getListenerContainer("foo").pause();
        Thread.sleep(5000);
        kafkaTemplate1.send("topic-1", 1, "1").get();
        kafkaTemplate1.send("topic-1", 2, "1").get();
        kafkaTemplate1.send("topic-1", 3, "1").get();
        kafkaTemplate1.send("topic-1", 4, "1").get();
        kafkaTemplate1.send("topic-1", 4, "1").get();
        registry.getListenerContainer("foo").resume();
        List<String> data = resultFuture1.get(20, TimeUnit.SECONDS);
        assertEquals(5, data.size());
    }

}