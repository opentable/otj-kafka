package com.opentable.kafka.builders;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;

public class KafkaBuilder {

    final Properties prop;

    KafkaBuilder(Properties prop) {
        this.prop = prop;
    }

    public Properties buildProps() {
        return prop;
    }

    public static KafkaBuilder builder() {
        return new KafkaBuilder(new Properties());
    }

    public static KafkaBuilder builder(Properties props) {
        return new KafkaBuilder(props);
    }

    public KafkaBuilder withProp(String key, Object value) {
        prop.put(key, value);
        return this;
    }

    public KafkaBuilder withoutProp(String key) {
        prop.remove(key);
        return this;
    }

    public KafkaBuilder withBootstrapServers(String val) {
        return withProp(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, val);
    }

    public KafkaBuilder withBootstrapServer(String val) {
        setListPropItem(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, val);
        return this;
    }
    public KafkaBuilder withClientId(String val) {
        return withProp(CommonClientConfigs.CLIENT_ID_CONFIG, val);
    }

    public KafkaBuilder withSecurityProtocol(String val) {
        return withProp(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, val);
    }

    public KafkaBuilder withMetricReporter(MetricRegistry metricRegistry) {
        setListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getCanonicalName());
        return withProp(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG, metricRegistry);
    }

    public KafkaBuilder withMetricReporter() {
        setListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getCanonicalName());
        return this;
    }

    public KafkaBuilder withoutMetricReporter() {
        removeListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getCanonicalName());
        return this;
    }

    protected void setListPropItem(String key, String val) {
        prop.put(key,
            Streams.concat(Arrays.stream(prop.getProperty(key, "").split(",")),
                Stream.of(val))
                .filter(s -> !s.equals(""))
                .distinct()
                .collect(Collectors.joining(",")));
    }

    protected void removeListPropItem(String key, String val) {
        prop.put(key,
            Arrays.stream(prop.getProperty(key, "").split(","))
                .filter(s -> !s.equals(""))
                .filter(s -> !s.equals(val))
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
