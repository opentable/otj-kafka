package com.opentable.kafka.spring.features;


import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.opentable.jackson.OpenTableJacksonConfiguration;
import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.spring.AbstractTest;
import com.opentable.kafka.spring.builders.KafkaFactoryBuilderFactoryBean;

@ContextConfiguration(classes = {JsonTest.Config.class, OpenTableJacksonConfiguration.class})
@TestPropertySource(properties = {
    "ot.kafka.consumer.test.ack-mode=MANUAL",
    "ot.kafka.consumer.test.concurrency=1"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class JsonTest extends AbstractTest {


    public static class TestDTO {
        private Instant instant;
        private String string;
        private Integer integer;

        @JsonCreator
        public TestDTO(Instant instant, String string, Integer integer) {
            this.instant = instant;
            this.string = string;
            this.integer = integer;
        }

        public Instant getInstant() {
            return instant;
        }

        public void setInstant(Instant instant) {
            this.instant = instant;
        }

        public String getString() {
            return string;
        }

        public void setString(String string) {
            this.string = string;
        }

        public Integer getInteger() {
            return integer;
        }

        public void setInteger(Integer integer) {
            this.integer = integer;
        }
    }

    public static class TestDTO2 {
        private String blah;

        @JsonCreator
        public TestDTO2(String blah) {
            this.blah = blah;
        }

        public String getBlah() {
            return blah;
        }

        public void setBlah(String blah) {
            this.blah = blah;
        }
    }


    @Autowired
    private KafkaTemplate<Integer, Object> kafkaTemplate1;

    @Autowired
    @Qualifier("resultFuture1")
    private SettableListenableFuture<TestDTO> resultFuture1;

    @Autowired
    @Qualifier("resultFuture2")
    private SettableListenableFuture<TestDTO2> resultFuture2;


    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Configuration
    @EnableKafka
    public static class Config {

        @Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
        protected String brokerAddresses;

        @Autowired
        private KafkaFactoryBuilderFactoryBean kafkaFactoryBuilderFactoryBean;

        @Autowired
        private ObjectMapper objectMapper;

        @Bean
        public ProducerFactory<Integer, Object> producerFactory1() {
            return kafkaFactoryBuilderFactoryBean.producerFactoryBuilder("test")
                .withBootstrapServer(brokerAddresses)
                .build(new IntegerSerializer(), new JsonSerializer<>(objectMapper));
        }

        @Bean
        public KafkaTemplate<Integer, Object> kafkaTemplate1() {
            return new KafkaTemplate<>(producerFactory1());
        }

        @Bean
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, Object>> kafkaListenerContainerFactory() {
            return kafkaFactoryBuilderFactoryBean.consumerFactoryBuilder("test")
                .withBootstrapServer(brokerAddresses)
                .withJsonMessageConverter(IntegerDeserializer.class)
                .withAutoOffsetReset(KafkaConsumerBuilder.AutoOffsetResetType.Earliest)
                .withBatchListener(false)
                .build();
        }

        @Bean
        @Qualifier("resultFuture1")
        public SettableListenableFuture<TestDTO> resultFuture1() {
            return new SettableListenableFuture<>();
        }

        @Bean
        @Qualifier("resultFuture2")
        public SettableListenableFuture<TestDTO2> resultFuture2() {
            return new SettableListenableFuture<>();
        }

        @KafkaListener(id = "foo", topics = "topic-1", containerFactory = "kafkaListenerContainerFactory", concurrency = "1")
        public void listen(TestDTO data) {
            resultFuture1().set(data);
        }

        @KafkaListener(id = "foo2", topics = "topic-2", containerFactory = "kafkaListenerContainerFactory", concurrency = "1")
        public void listen2(TestDTO2 data) {
            resultFuture2().set(data);
        }
    }

    @Test
    public void kafkaJsonProducerConsumerTest() throws ExecutionException, InterruptedException, TimeoutException {
        Instant instant = Instant.now();
        kafkaTemplate1.send("topic-2", 1, new TestDTO2("blah")).get();
        kafkaTemplate1.send("topic-1", 1, new TestDTO(instant, "test", 1234)).get();
        TestDTO data = resultFuture1.get(20, TimeUnit.SECONDS);
        assertEquals("test", data.getString());
        assertEquals(1234, data.getInteger().intValue());
        assertEquals(instant, data.getInstant());
        TestDTO2 data2 = resultFuture2.get(20, TimeUnit.SECONDS);
        assertEquals("blah", data2.getBlah());
    }

    @Test
    public void kafkaJsonMessageProducerTest() throws ExecutionException, InterruptedException, TimeoutException {
        Instant instant = Instant.now();
        kafkaTemplate1.send(MessageBuilder
            .withPayload(new TestDTO(instant, "test2", 12345))
            .setHeader(KafkaHeaders.TOPIC, "topic-1")
            .setHeader(KafkaHeaders.MESSAGE_KEY, 1)
            .build()).get();
        TestDTO data = resultFuture1.get(20, TimeUnit.SECONDS);
        assertEquals("test2", data.getString());
        assertEquals(12345, data.getInteger().intValue());
        assertEquals(instant, data.getInstant());
    }

}