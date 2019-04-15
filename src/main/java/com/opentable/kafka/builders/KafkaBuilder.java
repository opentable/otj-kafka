package com.opentable.kafka.builders;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.opentable.kafka.metrics.OtMetricsReporter;

public class KafkaBuilder {

    final Properties prop;

    KafkaBuilder(Properties prop) {
        this.prop = prop;
    }

    public Properties buildProps() {
        return new Properties(prop);
    }

    public static KafkaBuilder builder() {
        return new KafkaBuilder(new Properties());
    }

    public KafkaBuilder withProp(Object key, Object value) {
        prop.put(key, value);
        return this;
    }

    public KafkaBuilder withoutProp(Object key) {
        prop.remove(key);
        return this;
    }

    public KafkaBuilder withBootstrapServers(String val) {
        return withProp(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, val);
    }

    public KafkaBuilder withClientId(String val) {
        return withProp(CommonClientConfigs.CLIENT_ID_CONFIG, val);
    }

    public KafkaBuilder withSecurityProtocol(String val) {
        return withProp(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, val);
    }

    public KafkaBuilder withMetricReporter(MetricRegistry metricRegistry) {
        return withProp(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getCanonicalName())
            .withProp(OtMetricsReporter.METRIC_REPORTER_OT_REGISTRY, metricRegistry);
    }

    protected void setCsvProp(String key, String val) {
        prop.put(key,
            Streams.concat(Arrays.stream(prop.getProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "").split(",")),
                Stream.of(val))
                .filter(s -> !s.equals(""))
                .distinct()
                .collect(Collectors.joining(",")));
    }

    public KafkaProducerBuilder<?, ?> producer() {
        return new KafkaProducerBuilder<>(prop);
    }

    public KafkaConsumerBuilder<?, ?> consumer() {
        return new KafkaConsumerBuilder<>(prop);
    }

}
