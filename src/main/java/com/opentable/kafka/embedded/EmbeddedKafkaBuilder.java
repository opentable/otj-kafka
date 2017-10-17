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
package com.opentable.kafka.embedded;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmbeddedKafkaBuilder {
    private boolean autoCreateTopics;
    private final List<String> topicsToCreate = new ArrayList<>();

    public EmbeddedKafkaBuilder withTopics(String... topics) {
        topicsToCreate.addAll(Arrays.asList(topics));
        return this;
    }

    public EmbeddedKafkaBuilder autoCreateTopics(final boolean autoCreateTopics) {
        this.autoCreateTopics = autoCreateTopics;
        return this;
    }

    EmbeddedKafkaBroker build() {
        return new EmbeddedKafkaBroker(topicsToCreate, autoCreateTopics);
    }

    public EmbeddedKafkaBroker start() {
        return build().start();
    }

    public EmbeddedKafkaRule rule() {
        return new EmbeddedKafkaRule(build());
    }
}
