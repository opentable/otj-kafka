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
package com.opentable.kafka.session;

import java.util.List;

public class TestResult {
    private final List<ConsumedEvent<String>> events;
    private final List<ConsumerTask<String>> consumerTaskList;
    private final ProducerTask producerTask;

    public TestResult(final List<ConsumerTask<String>> consumerTaskList, final List<ConsumedEvent<String>> events, final ProducerTask producerTask) {
        this.events = events;
        this.consumerTaskList = consumerTaskList;
        this.producerTask = producerTask;
    }

    public List<ConsumerTask<String>> getConsumerTaskList() {
        return consumerTaskList;
    }

    public List<ConsumedEvent<String>> getEvents() {
        return events;
    }

    public ProducerTask getProducerTask() {
        return producerTask;
    }
}
