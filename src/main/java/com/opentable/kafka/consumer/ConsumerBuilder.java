package com.opentable.kafka.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

// This has no springiness it's just a basic builder.
// Added to this many options, validation during build and setting, etc.
public class ConsumerBuilder<K,V> {
    private MetricRegistry metricRegistry;
    private List<String> brokerList;
    private String consumerGroup;
    private String application;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private AutoOffsetResetMode autoOffsetResetMode = AutoOffsetResetMode.NONE;
    // And many more

    public enum AutoOffsetResetMode {
        NONE("none"),
        LATEST("latest"),
        EARLIEST("earliest")
        ;

        private final String mode;

        AutoOffsetResetMode(String mode) {
            this.mode = mode;
        }

        public String getMode() {
            return mode;
        }
    }

    public ConsumerBuilder withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public ConsumerBuilder withBrokers(String ... brokers ) {
         this.brokerList = Arrays.asList(brokers);
         return this;
    }

    public ConsumerBuilder withOffsetResetMode(AutoOffsetResetMode offsetStrategy) {
        this.autoOffsetResetMode = offsetStrategy;
        return this;
    }

    public ConsumerBuilder setApplicationName(String application) {
        this.application = application;
        return this;
    }

    public ConsumerBuilder setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    // And more

    public DelegatingConsumer<K,V> build() {
        // Do various validations and a lot more stuff
        return new DelegatingConsumer<K, V>(makeConsumer(), metricRegistry);
    }

    KafkaConsumer<K, V> makeConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList.stream().collect(Collectors.joining(",")));
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
       // props.put(ConsumerConfig.CLIENT_ID_CONFIG, makeClientId(topicPartition));
       // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetMode.getMode());
      //  final Deserializer<byte[]> keyDeserializer = new ByteArrayDeserializer();
        final KafkaConsumer<K,V> consumer = new KafkaConsumer<K,V>(props, keyDeserializer, valueDeserializer);
        return consumer;
    }
}
