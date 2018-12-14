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
package com.opentable.kafka.partition;

/**
    Use this to create a bean providing the basic configuration needed
 */
public class BrokerConfig {
    /**
     * Comma delimited list of brokers
     */
    private final String brokerList;
    /**
     * topic
     */
    private final String topic; // topic
    /**
     * is it enabled? Should be true for "normal" ops, false for when testing without kafka
     */
    private final boolean enabled;

    public BrokerConfig(final String brokerList, final String topic, final boolean kafkaEnabled) {
        this.brokerList = brokerList;
        this.topic = topic;
        this.enabled = kafkaEnabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerList() {
        return brokerList;
    }
}
