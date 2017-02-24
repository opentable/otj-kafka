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
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to rate-limit logging messages.
 */
public class LogLimiter {
    private final Logger LOG = LoggerFactory.getLogger(LogLimiter.class);

    private final String name;
    private final long maxCount;

    private final ConcurrentMap<String, AtomicLong> logsByKey = new ConcurrentHashMap<>();

    private volatile Instant resetAt = Instant.now();

    LogLimiter(String name, long maxCount) {
        this.name = name;
        this.maxCount = maxCount;
    }

    /**
     * Limit logging after a counter is exceeded.
     * @param name an informative name to log
     * @param maxCount the number of times to log by key
     */
    public static LogLimiter byCount(String name, long maxCount) {
        return new LogLimiter(name, maxCount);
    }

    /**
     * Mark a log event and return whether it should log.
     * @param key the key to mark
     * @return whether to log the event
     */
    public boolean mark(String key) {
        long count = logsByKey.computeIfAbsent(key, k -> new AtomicLong()).getAndIncrement();

        if (count < maxCount) {
            return true;
        }

        if (count == maxCount) {
            LOG.error("Reporting errors of type '{}' exceeded rate limit, "
                    + "suppressing further error message reporting for this key until reset", key);
        }

        return false;
    }

    /**
     * Reset counts and log a summary description.
     */
    public void clear() {
        Duration sinceReset = Duration.between(resetAt, Instant.now());

        logsByKey.forEach((k, c) -> {
            if (c.get() >= maxCount) {
                LOG.warn("Limiter '{}'.'{}' observed {} errors since {}", name, k, c, sinceReset);
            }
        });

        logsByKey.clear();
        resetAt = Instant.now();
    }
}
