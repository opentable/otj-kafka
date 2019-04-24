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
package com.opentable.kafka.logging;

import java.util.Optional;

import com.opentable.conservedheaders.ConservedHeader;

/**
 * Represents the names of various headers in Kafka. Ones marked appropriately map to a conserved header
 */
public enum OTKafkaHeaders {
    REFERRING_HOST("ot-kafkalibrary-referring-host"),
    REFERRING_INSTANCE_NO("ot-kafkalibrary-referring-instance-no"),
    REFERRING_SERVICE("ot-kafkalibrary-referring-service"),
    REQUEST_ID(ConservedHeader.REQUEST_ID.getHeaderName(), ConservedHeader.REQUEST_ID),
    TRACE_FLAG("ot-kafkalibrary-trace-flag"),
    ENV("ot-kafkalibrary-env"),
    ENV_FLAVOR("ot-kafkalibrary-env-flavor")
    ;

    // The namespacing logic is as follows
    // 1. All library usage (this library or others) preface with ot-
    // 2. Then the library name (kafkalibrary in this case)
    // 3. Then whatever you want
    // 4. This helps prevent collision
    private String kafkaName;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<ConservedHeader> conservedHeader;

    OTKafkaHeaders(String kafkaName) {
        this(kafkaName, null);
    }

    OTKafkaHeaders(String kafkaName, ConservedHeader conservedHeaderName) {
        this.kafkaName = kafkaName;
        this.conservedHeader = Optional.ofNullable(conservedHeaderName);
    }

    public String getKafkaName() {
        return kafkaName;
    }

    public Optional<ConservedHeader> getConservedHeader() {
        return conservedHeader;
    }
}
