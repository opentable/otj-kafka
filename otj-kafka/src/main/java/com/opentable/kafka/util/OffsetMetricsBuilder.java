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
package com.opentable.kafka.util;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.google.common.annotations.VisibleForTesting;
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
    private Function<String, Map<Integer, Long>> offsetsSupplier;

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

    public OffsetMetricsBuilder addTopics(final Collection<String> topics) {
        this.topics.addAll(topics);
        return this;
    }

    public OffsetMetricsBuilder addTopics(final String... topics) {
        return addTopics(Arrays.asList(topics));
    }

    public OffsetMetricsBuilder addTopic(final String topic) {
        return addTopics(Collections.singleton(topic));
    }

    /**
     * Customize the histogram reservoirs if you like; low-rate topics may benefit from a sliding time window reservoir
     * instead of an exponentially-decaying one, especially if you are adding alerts based on these metrics.
     * @param reservoirSupplier supplier
     * @return builder
     */
    public OffsetMetricsBuilder withReservoirs(final Supplier<Reservoir> reservoirSupplier) {
        this.reservoirSupplier = reservoirSupplier;
        return this;
    }

    /**
     * Use this if you are managing your own offsets. Given a topic, the map should yield partition to offset.
     * @param offsetsSupplier topic ... map(partition, offset)
     * @return builder
     */
    public OffsetMetricsBuilder withOffsetsSupplier(final Function<String, Map<Integer, Long>> offsetsSupplier) {
        this.offsetsSupplier = offsetsSupplier;
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
                pollPeriod,
                offsetsSupplier
        );
    }
}
