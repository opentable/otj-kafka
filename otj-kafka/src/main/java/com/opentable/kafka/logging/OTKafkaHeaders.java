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

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.opentable.conservedheaders.ConservedHeader;

/**
 * Represents the names of various headers in Kafka.
 * These are namespaced to avoid collisions, except for request Id
 */
public enum OTKafkaHeaders {

    REFERRING_HOST("ot-kafkalibrary-referring-host"),
    REFERRING_INSTANCE_NO("ot-kafkalibrary-referring-instance-no"),
    REFERRING_SERVICE("ot-kafkalibrary-referring-service"),
    REQUEST_ID(ConservedHeader.REQUEST_ID.getLogName()),
    TRACE_FLAG("ot-kafkalibrary-trace-flag"),
    ENV("ot-kafkalibrary-env"),
    ENV_FLAVOR("ot-kafkalibrary-env-flavor"),
    TRACE_ID("ot-kafkalibrary-ot-traceid"),
    SPAN_ID("ot-kafkalibrary-ot-spanid"),
    PARENT_SPAN_ID("ot-kafkalibrary-ot-parent-spanid"),
    PARENT_INHERITANCE_TYPE("ot-kafkalibrary-it-parent-inheritance")
    ;

    private String kafkaName;


    OTKafkaHeaders(String kafkaName) {
        this.kafkaName = kafkaName;
    }

    public String getKafkaName() {
        return kafkaName;
    }

    private static final Set<String> DEFINED_HEADERS = ConcurrentHashMap.newKeySet();

    public static boolean isDefinedHeader(String key) {
        return DEFINED_HEADERS.contains(key);
    }

    static {
        // Add the OTKafkaHeaders
        DEFINED_HEADERS.addAll(Arrays.stream(OTKafkaHeaders.values()).map(OTKafkaHeaders::getKafkaName).collect(Collectors.toSet()));
    }

}
