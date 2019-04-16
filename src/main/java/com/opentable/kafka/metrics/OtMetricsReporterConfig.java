package com.opentable.kafka.metrics;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class OtMetricsReporterConfig extends AbstractConfig {

    public static final String DEFAULT_REGISTRY_NAME = "default";
    public static final String METRIC_REGISTRY_NAME_CONFIG = "metric.reporter.ot.registry-name";
    public static final String METRIC_REGISTRY_REF_CONFIG = "metric.reporter.ot.registry";

    public static final String DEFAULT_PREFIX = "kafka.";
    public static final String METRIC_PREFIX_CONFIG = "metric.reporter.ot.prefix";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(METRIC_REGISTRY_NAME_CONFIG, ConfigDef.Type.STRING, DEFAULT_REGISTRY_NAME, ConfigDef.Importance.LOW,
            "Name of the dropwizard-metrics registry to use; passed to SharedMetricRegistries.getOrCreate")
        .define(METRIC_PREFIX_CONFIG, ConfigDef.Type.STRING, DEFAULT_PREFIX, ConfigDef.Importance.LOW,
            "Metric prefix for metrics published by the OtMetricsReporter");

    public OtMetricsReporterConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}
