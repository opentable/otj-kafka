package com.opentable.kafka.metrics;

import java.util.List;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

public class OtMetricsReporter implements MetricsReporter {

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
        metricRegistry = (MetricRegistry) configs.get("metric.registry");
    }
}
