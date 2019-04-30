package com.opentable.kafka.builders;

import java.time.Duration;
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

public abstract class KafkaBuilder<SELF extends KafkaBuilder<SELF>> extends SelfTyped<SELF> {

    final Properties prop;

    KafkaBuilder(Properties prop) {
        this.prop = prop;
    }

    public Properties buildProps() {
        return prop;
    }

    public SELF withProperty(String key, Object value) {
        prop.put(key, value);
        return self();
    }

    public SELF removeProperty(String key) {
        prop.remove(key);
        return self();
    }

    public SELF withBootstrapServers(String val) {
        return withProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, val);
    }

    public SELF withBootstrapServer(String val) {
        setListPropItem(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, val);
        return self();
    }
    public SELF withClientId(String val) {
        return withProperty(CommonClientConfigs.CLIENT_ID_CONFIG, val);
    }

    public SELF withSecurityProtocol(String val) {
        return withProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, val);
    }

    public SELF withMetricReporter(MetricRegistry metricRegistry) {
        setListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
        return withProperty(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG, metricRegistry);
    }

    public SELF withMetricReporter() {
        setListPropItem(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
        return self();
    }

    public SELF disableMetricReporter() {
        removeListPropItem(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
        return self();
    }

    public SELF withRequestTimeout(Duration val) {
        return withProperty(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, val.toMillis());
    }

    public SELF withRetryBackoff(Duration val) {
        return withProperty(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, val.toMillis());
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

}
