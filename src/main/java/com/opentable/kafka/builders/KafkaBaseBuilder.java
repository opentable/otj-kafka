package com.opentable.kafka.builders;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingUtils;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.service.AppInfo;

public class KafkaBaseBuilder {
    final LoggingUtils loggingUtils;
    Double loggingSampleRate = LoggingInterceptorConfig.DEFAULT_SAMPLE_RATE_PCT;
    final List<String> interceptors = new ArrayList<>();
    final Properties prop;
    private final List<String> bootStrapServers = new ArrayList<>();
    private Optional<String> clientId = Optional.empty();
    private Optional<String> securityProtocol = Optional.empty();
    protected Optional<MetricRegistry> metricRegistry;

    KafkaBaseBuilder(Properties prop, AppInfo appInfo) {
        this.prop = prop;
        this.loggingUtils = new LoggingUtils(appInfo);
        metricRegistry = Optional.empty();
    }


    void addProperty(String key, Object value) {
        prop.put(key, value);
    }

    void removeProperty(String key) {
        prop.remove(key);
    }

    void withBootstrapServer(String bootStrapServer) {
        this.bootStrapServers.add(bootStrapServer);
    }

    void withBootstrapServers(List<String> bootStrapServers) {
        this.bootStrapServers.addAll(bootStrapServers);
    }

    void withClientId(String val) {
        clientId = Optional.ofNullable(val);
    }

    void withSecurityProtocol(String protocol) {
        this.securityProtocol = Optional.ofNullable(protocol);
    }

    void withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = Optional.ofNullable(metricRegistry);
    }

    void baseBuild() {
        if (bootStrapServers.isEmpty()) {
            throw new IllegalStateException("No bootstrap servers specified");
        }
        addProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers.stream().distinct().collect(Collectors.joining(",")));
        clientId.ifPresent(cid -> addProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, cid));
        securityProtocol.ifPresent(sp -> addProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp));
        metricRegistry.ifPresent(mr -> {
            addProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, OtMetricsReporter.class.getName());
            addProperty(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG, mr);
        });
    }
}
