package com.opentable.kafka.util;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

/**
 * Get one of these by calling {@link OffsetMetrics#builder}; provide at least one topic.
 */
public class OffsetMetricsBuilder {
    private final Set<String> topics = new HashSet<>();

    private final String metricPrefix;
    private final MetricRegistry metricRegistry;
    private final String groupId;
    private final String brokerList;

    private Duration pollPeriod = Duration.ofSeconds(10);
    private Supplier<Reservoir> reservoirSupplier = ExponentiallyDecayingReservoir::new;

    OffsetMetricsBuilder(
            final String metricPrefix,
            final MetricRegistry metricRegistry,
            final String groupId,
            final String brokerList
    ) {
        this.metricPrefix = metricPrefix;
        this.metricRegistry = metricRegistry;
        this.groupId = groupId;
        this.brokerList = brokerList;
    }

    public OffsetMetricsBuilder withTopics(final Collection<String> topics) {
        this.topics.addAll(topics);
        return this;
    }

    public OffsetMetricsBuilder withTopics(final String... topics) {
        return withTopics(Arrays.asList(topics));
    }

    public OffsetMetricsBuilder withTopic(final String topic) {
        return withTopics(Collections.singleton(topic));
    }

    /**
     * Customize the histogram reservoirs if you like; low-rate topics may benefit from a sliding time window reservoir
     * instead of an exponentially-decaying one, especially if you are adding alerts based on these metrics.
     */
    public OffsetMetricsBuilder withReservoirs(final Supplier<Reservoir> reservoirSupplier) {
        this.reservoirSupplier = reservoirSupplier;
        return this;
    }

    @VisibleForTesting
    OffsetMetricsBuilder withPollPeriod(final Duration pollPeriod) {
        this.pollPeriod = pollPeriod;
        return this;
    }

    public OffsetMetrics build() {
        return new OffsetMetrics(
                metricPrefix,
                metricRegistry,
                groupId,
                brokerList,
                ImmutableSet.copyOf(topics),
                reservoirSupplier,
                pollPeriod
        );
    }
}
