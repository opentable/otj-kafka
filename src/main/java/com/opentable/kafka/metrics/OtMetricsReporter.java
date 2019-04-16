package com.opentable.kafka.metrics;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;

public class OtMetricsReporter implements MetricsReporter {

    private static final Logger LOG = LoggerFactory.getLogger(OtMetricsReporter.class);

    private OtMetricsReporterConfig config;
    private Set<String> metricNames = new HashSet<>();
    private Set<String> metricGroups = new HashSet<>();
    private MetricRegistry metricRegistry;
    private String groupId;
    private String prefix;
    private final HashSet<String> groups = new HashSet<>();
    private final HashSet<String> metricMatchers = new HashSet<>();
    private final AntPathMatcher matcher = new AntPathMatcher("-");

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (filterMetric(metric)) {
            final String name = metricName(metric);
            try {
                metricRegistry.register(name, (Gauge) metric::metricValue);
                metricNames.add(name);
                metricGroups.add(metric.metricName().group());
            } catch (IllegalArgumentException e) {
                LOG.warn("metricChange called for `{}' which was already registered, ignoring.", name);
            }
        }
    }

    private boolean filterMetric(KafkaMetric metric) {
        return filterGroup(metric) && filterName(metric);
    }

    private boolean filterGroup(KafkaMetric metric) {
        if (groups.isEmpty()) {
            return true;
        }
        return groups.contains(metric.metricName().group());
    }

    private boolean filterName(KafkaMetric metric) {
        if (metricMatchers.isEmpty()) {
            return true;
        }
        return metricMatchers.stream()
            .anyMatch(m -> matcher.match(m, metric.metricName().name()));
    }

    private String metricName(KafkaMetric metric) {
        final StringBuilder stringBuilder = new StringBuilder(prefix);
        stringBuilder.append(metric.metricName().tags().get("client-id"))
            .append(".");
        if (groupId != null) {
            stringBuilder.append(groupId)
                .append(".");
        }
        metric.metricName().tags().entrySet().stream()
            .filter(v -> !"client-id".equals(v.getKey()))
            .sorted(Comparator.comparing(Entry::getKey))
            .forEach(v -> stringBuilder.append(v.getValue()).append("."));
        stringBuilder.append(metric.metricName().group())
            .append("-")
            .append(metric.metricName().name());
        return stringBuilder.toString();
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        final String name = metricName(metric);
        metricRegistry.remove(name);
        metricNames.remove(name);
    }

    @Override
    public void close() {
        metricNames.forEach(name -> {
            LOG.trace("Un-registering kafka metric: {}", name);
            metricRegistry.remove(name);
        });
        metricNames.clear();
        LOG.debug("Metric groups: {}", metricGroups);
        metricGroups.clear();
    }

    @Override
    public void configure(Map<String, ?> config) {
        this.config = new OtMetricsReporterConfig(config);
        this.metricRegistry  = (MetricRegistry) config.get(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG);
        if (this.metricRegistry == null) {
            final String registryName = this.config.getString(OtMetricsReporterConfig.METRIC_REGISTRY_NAME_CONFIG);
            this.metricRegistry = SharedMetricRegistries.getOrCreate(registryName);
        }
        this.prefix = this.config.getString(OtMetricsReporterConfig.METRIC_PREFIX_CONFIG);
        groups.addAll(this.config.getList(OtMetricsReporterConfig.METRIC_GROUPS_CONFIG));
        metricMatchers.addAll(this.config.getList(OtMetricsReporterConfig.METRIC_NAME_MATCHERS_CONFIG));
        groupId  = (String)config.get(ConsumerConfig.GROUP_ID_CONFIG);
        LOG.info("OtMetricsReporter is configured with metric registry: {} and prefix: {}", metricRegistry, prefix);
    }

}
