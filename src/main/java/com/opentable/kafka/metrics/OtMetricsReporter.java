package com.opentable.kafka.metrics;

import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

public class OtMetricsReporter implements MetricsReporter {

    public static final String METRIC_REPORTER_OT_REGISTRY = "metric.reporter.ot.registry";
    private MetricRegistry metricRegistry;

    @Override
    public void init(List<KafkaMetric> metrics) {

    }

    @Override
    public void metricChange(KafkaMetric metric) {

    }

    @Override
    public void metricRemoval(KafkaMetric metric) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        metricRegistry = (MetricRegistry) configs.get(METRIC_REPORTER_OT_REGISTRY);
    }
}
