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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;

public class OtMetricsReporter implements MetricsReporter {

    private static final Logger LOG = LoggerFactory.getLogger(OtMetricsReporter.class);
    private static final Pattern blacklistedChars = Pattern.compile("[{}(),=\\[\\]/]");

    private final Set<String> metricNames = new HashSet<>();
    private final Set<String> metricGroups = new HashSet<>();
    private final Map<String, String> metricTags = new HashMap<>();
    private MetricRegistry metricRegistry;
    private String groupId;
    private String prefix;
    private final Set<String> groups = new HashSet<>();
    private final Set<String> metricMatchers = new HashSet<>();
    private final AntPathMatcher matcher = new AntPathMatcher("-");

    public OtMetricsReporter() { //NOPMD
        /* no args needed for kafka */
    }

    @Override
    public synchronized void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public synchronized void metricChange(KafkaMetric metric) {
        // If we qualify to use this metric ...
        if (filterMetric(metric)) {
            // ... derive its name
            final String name = metricName(metric);
            try {
                // Map the Measureable or kafka.gauge type to a dropwizard Gauge<Object>
                if (!metricRegistry.getNames().contains(name)) {
                    metricRegistry.register(name, (Gauge) metric::metricValue);
                }
                // Add to known list. We continue on because another thread may have added to registry already.
                metricNames.add(name);
                // and its group
                metricGroups.add(metric.metricName().group());
                metricTags.putAll(metric.metricName().tags());
            } catch (IllegalArgumentException e) {
                // Might happen if multiple threads access the MetricRegistry simultaneously.
                LOG.warn("metricChange called for `{}' which was already registered, ignoring.", name);
            }
        }
    }

    private boolean filterMetric(KafkaMetric metric) {
        return filterGroup(metric) && filterName(metric);
    }

    /**
     * Whitelist - if there are any filter groups (which by default there are), must be contained in the set
     * @param metric metric
     * @return if no filter groups or contained in the set
     */
    private boolean filterGroup(KafkaMetric metric) {
        if (groups.isEmpty()) {
            return true;
        }
        return groups.contains(metric.metricName().group());
    }

    /**
     * Whitelist - if there are any metric matchers (there are none by default), must match one of them
     * @param metric metric
     * @return if no metric matchers or in set
     */
    private boolean filterName(KafkaMetric metric) {
        if (metricMatchers.isEmpty()) {
            return true;
        }
        // MJB: Note since this is called on all metrics, it could be rather expensive
        return metricMatchers.stream()
            .anyMatch(m -> matcher.match(m, metric.metricName().name()));
    }

    private String metricName(KafkaMetric metric) {
        final MetricName metricName = metric.metricName();
        return MetricRegistry.name(prefix,
            sanitize(metricName.tags().get("client-id")),
            sanitize(groupId),
            metricName.group(),
            metricName.name(),
            sanitize(metricName.tags().get("topic")),
            sanitize(metricName.tags().get("partition")));
    }

    @Override
    public synchronized void metricRemoval(KafkaMetric metric) {
        final String name = metricName(metric);
        metricRegistry.remove(name);
        metricNames.remove(name);
    }

    @Override
    public synchronized void close() {
        metricNames.forEach(name -> {
            LOG.debug("Un-registering kafka metric: {}", name);
            metricRegistry.remove(name);
        });
        metricNames.clear();
        LOG.debug("Metric groups: {}", metricGroups);
        metricGroups.clear();
        LOG.debug("Metric tags: {}", metricTags);
        metricTags.clear();
    }

    @Override
    public synchronized void configure(Map<String, ?> config) {
        // The normal path is that the metric registry was stored....
        final OtMetricsReporterConfig otMetricsReporterConfig = new OtMetricsReporterConfig(config);
        this.metricRegistry  = (MetricRegistry) config.get(OtMetricsReporterConfig.METRIC_REGISTRY_REF_CONFIG);
        // Alternatively if no metric registry was supplied, which implies a non spring environment, create one
        if (this.metricRegistry == null) {
            LOG.debug("No metric registry supplied, creating one");
            final String registryName = otMetricsReporterConfig.getString(OtMetricsReporterConfig.METRIC_REGISTRY_NAME_CONFIG);
            if (registryName == null) {
                throw new IllegalStateException("No METRIC_REGISTRY_NAME_CONFIG");
            }
            this.metricRegistry = SharedMetricRegistries.getOrCreate(registryName);
        }
        // Load the prefix, groups, and matchers
        this.prefix = otMetricsReporterConfig.getString(OtMetricsReporterConfig.METRIC_PREFIX_CONFIG);
        if (prefix == null) {
            throw new IllegalStateException("METRIC_PREFIX_CONFIG is null");
        }
        if (otMetricsReporterConfig.getList(OtMetricsReporterConfig.METRIC_GROUPS_CONFIG) != null) {
            groups.addAll(otMetricsReporterConfig.getList(OtMetricsReporterConfig.METRIC_GROUPS_CONFIG));
        }
        if (otMetricsReporterConfig.getList(OtMetricsReporterConfig.METRIC_NAME_MATCHERS_CONFIG) != null) {
            metricMatchers.addAll(otMetricsReporterConfig.getList(OtMetricsReporterConfig.METRIC_NAME_MATCHERS_CONFIG));
        }

        // and extract the groupId - we'll null check later
        groupId  = (String)config.get(ConsumerConfig.GROUP_ID_CONFIG);

        LOG.info("OtMetricsReporter is configured with metric registry: {} and prefix: {}", metricRegistry, prefix);
    }

    private String sanitize(String name) {
        if (name == null) {
            return null;
        }
        return blacklistedChars.matcher(name).replaceAll("_");
    }

}
