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
import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.logging.LogSampler.SamplerType;
import com.opentable.kafka.logging.LoggingInterceptorConfig;
import com.opentable.kafka.metrics.OtMetricsReporter;
import com.opentable.kafka.metrics.OtMetricsReporterConfig;

/**
 * Some common configuration options + the main properties builder is here.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class KafkaBaseBuilder<SELF extends KafkaBaseBuilder<SELF>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBaseBuilder.class);

    private final EnvironmentProvider environmentProvider;
    private final Map<String, Object> seedProperties; // what the user initialized with. This will take priority since mergeProperties will merge in last.
    private final Map<String, Object> finalProperties = new HashMap<>(); // what we'll build the final version of
    private final List<String> interceptors = new ArrayList<>();
    private boolean enableLoggingInterceptor = true;
    private int loggingSampleRate = LoggingInterceptorConfig.DEFAULT_SAMPLE_RATE_PCT;
    private int loggingDenominator = LoggingInterceptorConfig.DEFAULT_BUCKET_DENOMINATOR;
    private Optional<String> metricsPrefix = Optional.empty();
    private SamplerType loggingSamplerType = SamplerType.TimeBucket;
    private OptionalLong requestTimeout = OptionalLong.empty();
    private OptionalLong retryBackoff = OptionalLong.empty();
    private final List<String> bootStrapServers = new ArrayList<>();
    private Optional<String> clientId = Optional.empty();
    private Optional<SecurityProtocol> securityProtocol = Optional.empty();
    private Optional<MetricRegistry> metricRegistry;
    private boolean enableMetrics = true;

    protected KafkaBaseBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        this.seedProperties = props;
        this.environmentProvider = environmentProvider;
        metricRegistry = Optional.empty();
    }

    protected abstract SELF self();

    protected void addProperty(String key, Object value) {
        finalProperties.put(key, value);
    }

    protected void addInterceptor(String className) {
        interceptors.add(className);
    }

    protected void removeInterceptor(String className) {
        interceptors.remove(className);
    }

    protected void setupInterceptors(String interceptorConfigName, String loggingInterceptorName) {
        if (enableLoggingInterceptor) {
            interceptors.add(loggingInterceptorName);
        }
        // Copy over any injected properties, then remove the seed property
        List<String> merged = merge(interceptors, interceptorConfigName);
        if (merged.contains(loggingInterceptorName)) {
            if (loggingDenominator <= 0) {
                throw new IllegalArgumentException("LoggingDenominator must be > 0");
            }
            addProperty(LoggingInterceptorConfig.LOGGING_ENV_REF, environmentProvider);
            addProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, loggingSampleRate);
            addProperty(LoggingInterceptorConfig.SAMPLE_RATE_BUCKET_SECONDS_CONFIG, loggingDenominator);
            addProperty(LoggingInterceptorConfig.SAMPLE_RATE_TYPE_CONFIG, loggingSamplerType.getValue());
            LOG.debug("Setting sampler {} - {}, {} second bucket ", loggingSamplerType, loggingSampleRate,
                    loggingSamplerType == SamplerType.TimeBucket ? loggingDenominator : "--NA--");
        }
    }

    protected void setupBootstrap() {
        merge(bootStrapServers,CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    }

    protected List<String> merge(List<String> root, String mergePropertyName) {
        // Copy over any injected properties, then remove the seed property
        root.addAll(mergeListProperty(mergePropertyName));
        if (!root.isEmpty()) {
            addProperty(mergePropertyName, root.stream().distinct().collect(Collectors.joining(",")));
        }
        return root;
    }

    public SELF removeProperty(String key) {
        finalProperties.remove(key);
        return self();
    }

    public SELF withProperty(String key, Object value) {
        this.addProperty(key, value);
        return self();
    }


    public SELF withProperties(final Map<String, Object> config) {
        this.addProperties(config);
        return self();
    }

    protected SELF withPrefix(String metricsPrefix) {
        if ((metricsPrefix != null) && (!metricsPrefix.startsWith(OtMetricsReporterConfig.DEFAULT_PREFIX + "."))) {
                metricsPrefix = OtMetricsReporterConfig.DEFAULT_PREFIX + "." + metricsPrefix;
        }
        this.metricsPrefix = Optional.ofNullable(metricsPrefix);
        return self();
    }

    public SELF withBootstrapServer(String bootStrapServer) {
        this.bootStrapServers.add(bootStrapServer);
        return self();
    }

    public SELF withBootstrapServers(List<String> bootStrapServers) {
        this.bootStrapServers.addAll(bootStrapServers);
        return self();
    }

    public SELF withClientId(String val) {
        clientId = Optional.ofNullable(val);
        return self();
    }

    public SELF withSecurityProtocol(SecurityProtocol protocol) {
        this.securityProtocol = Optional.ofNullable(protocol);
        return self();
    }

    public SELF withRequestTimeoutMs(Duration duration) {
        requestTimeout = OptionalLong.of(duration.toMillis());
        return self();
    }

    public SELF withRequestTimeout(Duration duration) {
        if (duration != null) {
            withRequestTimeoutMs(duration);
        }
        return self();
    }

    public SELF withRetryBackOff(Duration duration) {
        retryBackoff = OptionalLong.of(duration.toMillis());
        return self();
    }

    public SELF withRetryBackoff(Duration duration) {
        if (duration != null) {
            withRetryBackOff(duration);
        }
        return self();
    }

    public SELF withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = Optional.ofNullable(metricRegistry);
        return self();
    }

    public SELF withMetricRegistry(MetricRegistry metricRegistry, String metricsPrefix) {
        withMetricRegistry(metricRegistry);
        withPrefix(metricsPrefix);
        return self();
    }

    protected SELF withMetrics(final boolean enabled) {
        this.enableMetrics = enabled;
        return self();
    }

    public SELF disableMetrics() {
        return withMetrics(false);
    }

    public SELF withLogging(boolean enabled) {
        enableLoggingInterceptor = enabled;
        return self();
    }

    public SELF withBucketedSamplingRate(final int rate, final int denominator) {
        loggingSampleRate = rate;
        loggingDenominator = denominator;
        loggingSamplerType = SamplerType.TimeBucket;
        return self();
    }

    public SELF withSamplingRatePer10Seconds(int rate) {
        return withBucketedSamplingRate(rate, LoggingInterceptorConfig.DEFAULT_BUCKET_DENOMINATOR);
    }

    public SELF withRandomSamplingRate(final int rate) {
        loggingSampleRate = rate;
        loggingSamplerType = SamplerType.Random;
        return self();
    }

    public SELF disableLogging() {
        return withLogging(false);
    }

    void finishBuild() {
        setupBootstrap();
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
           if (enableMetrics) {
               if (!metricsPrefix.isPresent()) {
                   throw new IllegalArgumentException("MetricsPrefix is not set");
               }
               metricReporters.add(OtMetricsReporter.class.getName());
               LOG.debug("Registering OTMetricsReporter for Kafka with prefix {}", metricsPrefix.get());
               addProperty(OtMetricsReporterConfig.METRIC_PREFIX_CONFIG, metricsPrefix.get());
               addProperty(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG, mr);
           }
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

    @VisibleForTesting
    Map<String, Object> getFinalProperties() {
        return finalProperties;
    }

    void addProperties(final Map<String, Object> config) {
        config.forEach(this::addProperty);
    }
}
