/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opentable.kafka.builders;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.logging.LoggingUtils;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;

/**
 * Some common configuration options + the main properties builder is here.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class KafkaBaseBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBaseBuilder.class);

    private final LoggingUtils loggingUtils;
    private final Map<String, Object> seedProperties; // what the user initialized with. This will take priority since mergeProperties will merge in last.
    private final Map<String, Object> finalProperties = new HashMap<>(); // what we'll build the final version of
    private final List<String> interceptors = new ArrayList<>();
    private boolean enableLoggingInterceptor = true;
    private int loggingSampleRate = LoggingInterceptorConfig.DEFAULT_SAMPLE_RATE_PCT;
    private OptionalLong requestTimeout = OptionalLong.empty();
    private OptionalLong retryBackoff = OptionalLong.empty();
    private final List<String> bootStrapServers = new ArrayList<>();
    private Optional<String> clientId = Optional.empty();
    private Optional<SecurityProtocol> securityProtocol = Optional.empty();
    private Optional<MetricRegistry> metricRegistry;

    KafkaBaseBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        this.seedProperties = props;
        this.loggingUtils = new LoggingUtils(environmentProvider);
        metricRegistry = Optional.empty();
    }


    void addProperty(String key, Object value) {
        finalProperties.put(key, value);
    }

    void addInterceptor(String className) {
        interceptors.add(className);
    }

    void removeInterceptor(String className) {
        interceptors.remove(className);
    }

    void setupInterceptors(String interceptorConfigName, String loggingInterceptorName) {
        if (enableLoggingInterceptor) {
            interceptors.add(loggingInterceptorName);
        }
        // Copy over any injected properties, then remove the seed property
        interceptors.addAll(mergeListProperty(interceptorConfigName));
        if (!interceptors.isEmpty()) {
            addProperty(interceptorConfigName, interceptors.stream().distinct().collect(Collectors.joining(",")));
            if (interceptors.contains(loggingInterceptorName)) {
                addProperty(LoggingInterceptorConfig.LOGGING_REF,  loggingUtils);
                addProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, loggingSampleRate);
            }
        }
    }



    void removeProperty(String key) {
        finalProperties.remove(key);
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

    void withSecurityProtocol(SecurityProtocol protocol) {
        this.securityProtocol = Optional.ofNullable(protocol);
    }

    void withRequestTimeoutMs(Duration duration) {
        requestTimeout = OptionalLong.of(duration.toMillis());
    }
    void withRetryBackOff(Duration duration) {
        retryBackoff = OptionalLong.of(duration.toMillis());
    }

    void withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = Optional.ofNullable(metricRegistry);
    }

    void withLogging(boolean enabled) {
        enableLoggingInterceptor = enabled;
    }
    void withSamplingRatePer10Seconds(final int rate) {
        loggingSampleRate = rate;
    }

    <CK,CV> KafkaConsumer<CK,CV> consumer() {
        LOG.trace("Building KafkaConsumer with props {}", finalProperties);
        return new KafkaConsumer<>(finalProperties);
    }

    <PK,PV> KafkaProducer<PK,PV> producer() {
        LOG.trace("Building KafkaProducer with props {}", finalProperties);
        return new KafkaProducer<>(finalProperties);
    }


    void finishBuild() {
        if (!bootStrapServers.isEmpty()) {
            addProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers.stream().distinct().collect(Collectors.joining(",")));
        }
        requestTimeout.ifPresent(i -> addProperty(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(i)));
        retryBackoff.ifPresent(i -> addProperty(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, String.valueOf(i)));
        clientId.ifPresent(cid -> addProperty(CommonClientConfigs.CLIENT_ID_CONFIG, cid));
        securityProtocol.ifPresent(sp -> addProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sp.name));
        setupMetrics();
        mergeProperties();
        cantBeNull(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "No bootstrap servers specified");
    }

    private void setupMetrics() {
        final List<String> metricReporters = mergeListProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG);
        metricRegistry.ifPresent(mr -> {
            metricReporters.add(OtMetricsReporter.class.getName());
            addProperty(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG, mr);
        });
        if (!metricReporters.isEmpty()) {
            addProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, metricReporters.stream().distinct().collect(Collectors.joining(",")));
        }
    }

    private List<String> mergeListProperty(String propertyName) {
        Object seededObject = seedProperties.get(propertyName);
        List<String> seedList = new ArrayList<>();
        if (seededObject instanceof String) {
            String seededList = (String) seededObject;
            seedList = Arrays.stream(seededList.split(",")).filter(StringUtils::isNotEmpty).collect(Collectors.toList());
        }
        // remove, because we've precombined, and don't want the mergeProperties() to overwrite.
        seedProperties.remove(propertyName);
        return seedList;
    }
    void cantBeNull(String key, String message) {
        if (finalProperties.get(key) == null) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * Merge the seedPropeties in. This gives preference to the seedProperties
     */
    private void mergeProperties() {
        seedProperties.forEach(finalProperties::put);
    }

}
