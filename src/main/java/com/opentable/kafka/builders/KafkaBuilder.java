package com.opentable.kafka.builders;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.opentable.kafka.logging.LoggingUtils;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.service.AppInfo;

public class KafkaBuilder {

    final Properties prop;
    final AppInfo appInfo;

    KafkaBuilder(Properties prop, AppInfo appInfo) {
        this.prop = prop;
        this.appInfo = appInfo;
        withProp("opentable.logging", new LoggingUtils(appInfo));
    }

    public Properties buildProps() {
        return prop;
    }

    public static KafkaBuilder builder(AppInfo appInfo) {
        return new KafkaBuilder(new Properties(), appInfo);
    }

    public static KafkaBuilder builder(Properties prop, AppInfo appInfo) {
        return new KafkaBuilder(prop, appInfo);
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
        setListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
        return withProp(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG, metricRegistry);
    }

    public KafkaBuilder withMetricReporter() {
        setListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
        return this;
    }

    public KafkaBuilder withoutMetricReporter() {
        removeListPropItem(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
        return this;
    }

    protected void setListPropItem(String key, String val) {
        prop.put(key,
            Stream.concat(Arrays.stream(prop.getProperty(key, "").split(",")),
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

    public <K,V> KafkaProducerBuilder<K, V> producer() {
        return new KafkaProducerBuilder<>(prop, appInfo);
    }

    public <K,V> KafkaConsumerBuilder<K, V> consumer() {
        return new KafkaConsumerBuilder<>(prop, appInfo);
    }

}
