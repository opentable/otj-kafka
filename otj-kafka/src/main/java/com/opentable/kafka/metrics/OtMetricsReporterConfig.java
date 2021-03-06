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
package com.opentable.kafka.metrics;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

public class OtMetricsReporterConfig extends AbstractConfig {

    private static final String DEFAULT_REGISTRY_NAME = "default";
    private static final String DEFAULT_METRIC_GROUPS = "producer-topic-metrics,producer-metrics,consumer-metrics,consumer-fetch-manager-metrics";
    private static final String DEFAULT_METRIC_NAME_MATCHERS = "";
    public static final String DEFAULT_PREFIX = "kafka";

    // Leave public for manual customization
    public static final String METRIC_REGISTRY_NAME_CONFIG = "ot.metric.reporter.registry.name";
    public static final String METRIC_REGISTRY_REF_CONFIG = "ot.metric.reporter.registry.reference";
    public static final String METRIC_GROUPS_CONFIG = "ot.metric.reporter.filter.groups";
    public static final String METRIC_NAME_MATCHERS_CONFIG = "ot.metric.reporter.filter.name.matchers";
    public static final String METRIC_PREFIX_CONFIG = "ot.metric.reporter.prefix";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(METRIC_REGISTRY_NAME_CONFIG, ConfigDef.Type.STRING, DEFAULT_REGISTRY_NAME, ConfigDef.Importance.LOW,
            "Name of the Dropwizard metrics registry to use; passed to SharedMetricRegistries.getOrCreate")
        .define(METRIC_PREFIX_CONFIG, ConfigDef.Type.STRING, DEFAULT_PREFIX, ConfigDef.Importance.LOW,
            "Metric prefix for metrics published by the OtMetricsReporter")
        .define(METRIC_GROUPS_CONFIG, Type.LIST, DEFAULT_METRIC_GROUPS, ConfigDef.Importance.LOW,
            "List of metric groups, which will be reported to Dropwizard.")
        .define(METRIC_NAME_MATCHERS_CONFIG, Type.LIST, DEFAULT_METRIC_NAME_MATCHERS, ConfigDef.Importance.LOW,
            "List of ant matchers to filter metric names, which will be reported to Dropwizard.");

    OtMetricsReporterConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}
