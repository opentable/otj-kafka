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
