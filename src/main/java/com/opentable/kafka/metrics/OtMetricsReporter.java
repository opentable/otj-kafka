package com.opentable.kafka.metrics;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtMetricsReporter implements MetricsReporter {

    private static final Logger LOG = LoggerFactory.getLogger(OtMetricsReporter.class);

    public static final String METRIC_REPORTER_OT_REGISTRY = "metric.reporter.ot.registry";

    private final Set<KafkaMetric> kafkaMetrics = new HashSet<>();
    private MetricRegistry metricRegistry;
    private String groupId;

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        synchronized (kafkaMetrics) {
            if (metricRegistry.getGauges().get(metricName(metric)) != null) {
                metricRegistry.remove(metricName(metric));
            }
            metricRegistry.register(metricName(metric), (Gauge) metric::metricValue);
        }
        kafkaMetrics.add(metric);
    }

    private String metricName(KafkaMetric metric) {
        final StringBuilder stringBuilder = new StringBuilder();
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
            .append(metric.metricName().name());
        return stringBuilder.toString();
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        metricRegistry.remove(metricName(metric));
        kafkaMetrics.remove(metric);
    }

    @Override
    public void close() {
        kafkaMetrics.forEach(metric -> {
            LOG.debug("Un-registering kafka metric: {}, tags: {}", metricName(metric), metric.metricName().tags());
            metricRegistry.remove(metricName(metric));
        });
        kafkaMetrics.clear();
    }

    @Override
    public void configure(Map<String, ?> config) {
        metricRegistry = (MetricRegistry) config.get(METRIC_REPORTER_OT_REGISTRY);
        groupId  = (String) config.get(ConsumerConfig.GROUP_ID_CONFIG);
        LOG.info("OtMetricsReporter is configured with metric registry: {}", metricRegistry);
    }

}
