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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a simple way to make sure all clientIds are unique.
 */
public class ClientIdGenerator {

    private static final ClientIdGenerator INSTANCE = new ClientIdGenerator();
    private final AtomicInteger consumerIds = new AtomicInteger(0);
    private final AtomicInteger producerIds = new AtomicInteger(0);
    private final AtomicInteger clientIds = new AtomicInteger(0);
    private final AtomicInteger metricReporterIds = new AtomicInteger(0);

    public int nextConsumerId() {
        return consumerIds.getAndIncrement();
    }

    public int nextPublisherId() {
        return producerIds.getAndIncrement();
    }

    public int nextClientId() {
        return clientIds.getAndIncrement();
    }

    public int nextMetricReporterId() {
        return metricReporterIds.getAndIncrement();
    }

    public static ClientIdGenerator getInstance() {
        return INSTANCE;
    }

}
