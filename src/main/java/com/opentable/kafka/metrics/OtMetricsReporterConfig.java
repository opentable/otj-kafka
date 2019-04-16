package com.opentable.kafka.metrics;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

public class OtMetricsReporterConfig extends AbstractConfig {

    private static final String DEFAULT_REGISTRY_NAME = "default";
    public static final String METRIC_REGISTRY_NAME_CONFIG = "metric.reporter.ot.registry-name";
    public static final String METRIC_REGISTRY_REF_CONFIG = "metric.reporter.ot.registry";

    private static final String DEFAULT_METRIC_GROUPS = "producer-topic-metrics,producer-metrics,consumer-metrics,consumer-fetch-manager-metrics";
    public static final String METRIC_GROUPS_CONFIG = "metric.reporter.ot.groups";

    private static final String DEFAULT_METRIC_NAME_MATCHERS = "";
    public static final String METRIC_NAME_MATCHERS_CONFIG = "metric.reporter.ot.name-matchers";

    private static final String DEFAULT_PREFIX = "kafka.";
    public static final String METRIC_PREFIX_CONFIG = "metric.reporter.ot.prefix";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(METRIC_REGISTRY_NAME_CONFIG, ConfigDef.Type.STRING, DEFAULT_REGISTRY_NAME, ConfigDef.Importance.LOW,
            "Name of the Dropwizard metrics registry to use; passed to SharedMetricRegistries.getOrCreate")
        .define(METRIC_PREFIX_CONFIG, ConfigDef.Type.STRING, DEFAULT_PREFIX, ConfigDef.Importance.LOW,
            "Metric prefix for metrics published by the OtMetricsReporter")
        .define(METRIC_GROUPS_CONFIG, Type.LIST, DEFAULT_METRIC_GROUPS, ConfigDef.Importance.LOW,
            "List of metric groups, which will be reported to the Dropwizard.")
        .define(METRIC_NAME_MATCHERS_CONFIG, Type.LIST, DEFAULT_METRIC_NAME_MATCHERS, ConfigDef.Importance.LOW,
            "List of ant matchers to filter metric names, which will be reported to the Dropwizard.");

    OtMetricsReporterConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}
