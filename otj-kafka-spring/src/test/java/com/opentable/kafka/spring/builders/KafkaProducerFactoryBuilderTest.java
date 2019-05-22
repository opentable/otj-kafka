package com.opentable.kafka.spring.builders;


import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.ContextConfiguration;

import com.opentable.kafka.spring.AbstractTest;
import com.opentable.kafka.spring.builders.KafkaProducerFactoryBuilderTest.Config;

@ContextConfiguration(classes = Config.class)
public class KafkaProducerFactoryBuilderTest extends AbstractTest {


    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate1;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate2;

    @Test
    public void kafkaTemplateTest() throws ExecutionException, InterruptedException {
       kafkaTemplate1.send("topic-i", 1, "1").get();
       kafkaTemplate2.send("topic-i", "2", "2").get();
    }

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
        public ProducerFactory<String, String> producerFactory2() {
            return kafkaFactoryBuilderFactoryBean.producerFactoryBuilder("test")
                .withBootstrapServer(brokerAddresses)
                .withSerializers(StringSerializer.class, StringSerializer.class)
                .build();
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate2() {
            return new KafkaTemplate<>(producerFactory2());
        }

    }

}