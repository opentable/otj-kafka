package com.opentable.kafka.builders;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingUtils;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;
import com.opentable.service.AppInfo;

/**
 * Some common configuration options + the main properties builder is here.
 */
public class KafkaBaseBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBaseBuilder.class);

    final LoggingUtils loggingUtils;
    Double loggingSampleRate = LoggingInterceptorConfig.DEFAULT_SAMPLE_RATE_PCT;
    final List<String> interceptors = new ArrayList<>();
    final Properties properties;
    private final List<String> bootStrapServers = new ArrayList<>();
    private Optional<String> clientId = Optional.empty();
    private Optional<String> securityProtocol = Optional.empty();
    protected Optional<MetricRegistry> metricRegistry;

    KafkaBaseBuilder(Properties props, AppInfo appInfo) {
        this.properties = props;
        this.loggingUtils = new LoggingUtils(appInfo);
        metricRegistry = Optional.empty();
    }


    void addProperty(String key, Object value) {
        properties.put(key, value);
    }

    void addLoggingUtilsRef(String interceptorConfigName, String loggingInterceptorName) {
        if (!interceptors.isEmpty()) {
            addProperty(interceptorConfigName, interceptors.stream().distinct().collect(Collectors.joining(",")));
            if (interceptors.contains(loggingInterceptorName)) {
                addProperty("opentable.logging",  loggingUtils);
                addProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, loggingSampleRate);
            }
        }
    }

    void removeProperty(String key) {
        properties.remove(key);
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

    <CK,CV> KafkaConsumer<CK,CV> consumer() {
        LOG.trace("Building KafkaConsumer with props {}", properties);
        return new KafkaConsumer<CK, CV>(properties);
    }

    <PK,PV> KafkaProducer<PK,PV> producer() {
        LOG.trace("Building KafkaProducer with props {}", properties);
        return new KafkaProducer<PK,PV>(properties);
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
