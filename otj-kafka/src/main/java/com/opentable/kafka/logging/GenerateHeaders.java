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

/**
 * Controls the generation of headers.
 * Default = ALL
 * Rationale is that for small messages, there's actually significant overhead here.
 */
public enum GenerateHeaders {
    ALL,  // Propagate headers including TRACE, conserved headers, and informational headers
    TRACE, // Propagate only the trace flag. Don't block user set stuff though
    NONE; // Propagate only user set headers, add none programmatically

    // Convert string in case insensitive manner. Default to ALL for null or non matching.
    public static GenerateHeaders fromString(String value) {
        return Arrays.stream(GenerateHeaders.values()).filter(t -> t.name().equalsIgnoreCase(value)).findFirst().orElse(GenerateHeaders.ALL);
    }
}
