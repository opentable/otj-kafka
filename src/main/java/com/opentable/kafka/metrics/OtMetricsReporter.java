package com.opentable.kafka.metrics;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

public class OtMetricsReporter implements MetricsReporter {

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

    }
}
