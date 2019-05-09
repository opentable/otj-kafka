package com.opentable.kafka.builders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Streams isn't really possible to build a builder for, but we can preconfigure some of the options.
 * Note: Some of the defaultConsumer/producer props may be inappropriate - Dmitry needs to check that.
 * Also I don't try to create a builder for Streams properties - that could be done of course.
 */
public class KafkaStreamsPropertiesBuilder {
    private final KafkaBuilderFactoryBean kafkaBuilderFactoryBean;

    @Inject
    public KafkaStreamsPropertiesBuilder(KafkaBuilderFactoryBean kafkaBuilderFactoryBean) {
        this.kafkaBuilderFactoryBean = kafkaBuilderFactoryBean;
    }

    public Properties getStreamsProperties(String name, List<String> bootstrapServers) {
        return getStreamsProperties(name, false, false, bootstrapServers);
    }

    //TODO: probably should be fronted with builder for StreamsConfig, this is a bit ugly.
    public Properties getStreamsProperties(String name, boolean disableMetrics, boolean disableLogging, List<String> bootStrapServers) {
        // Get the merged namespaced properties for consumer and producer specific
        final KafkaConsumerBuilder<?,?> kafkaConsumerBuilder = this.kafkaBuilderFactoryBean.consumerBuilder(name);
        final KafkaProducerBuilder<?,?> kafkaProducerBuilder = this.kafkaBuilderFactoryBean.producerBuilder(name);
        if (disableMetrics) {
            kafkaConsumerBuilder.disableMetrics();
            kafkaProducerBuilder.disableMetrics();
        }
        if (disableLogging) {
            kafkaConsumerBuilder.disableLogging();
            kafkaProducerBuilder.disableLogging();
        }
        kafkaConsumerBuilder.withBootstrapServers(bootStrapServers);
        final Map<String, Object> consumerStreamsProperties = kafkaConsumerBuilder.buildProperties();
        final Map<String, Object> producerStreamProperties = kafkaProducerBuilder.buildProperties();
        // Optional: Get Streams specific from properties?
        // Then merge them all - the other option is just for them to manage them externally.
        final Map<String, Object> mergedMap = new HashMap<>(consumerStreamsProperties);
        mergedMap.putAll(producerStreamProperties);
        // Streams doesn't allow this according to javadoc
        mergedMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        mergedMap.put(StreamsConfig.APPLICATION_ID_CONFIG, name);
        return toProperties(mergedMap);
    }

    private Properties toProperties(final Map<String, Object> mergedMap) {
        final Properties mergedProperties = new Properties();
        mergedMap.forEach(mergedProperties::put);
        return mergedProperties;
    }
}
