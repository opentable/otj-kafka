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


import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.local.LocalBucket;

/**
 * Generic log sampler
 */
public abstract class LogSampler {

    public abstract boolean mark(String key);

    static LogSampler create(LoggingInterceptorConfig conf) {
        switch (SamplerType.fromString(conf.getString(LoggingInterceptorConfig.SAMPLE_RATE_TYPE_CONFIG))) {
            case Random:
                return new LogSamplerRandom(conf);
            case TimeBucket:
                return new LogSamplerBucket(conf);
            default:
                throw new IllegalArgumentException("Unknown sampler type: " + conf.getString(LoggingInterceptorConfig.SAMPLE_RATE_TYPE_CONFIG));

        }

    }

    /**
     * Implementation based on time bucket
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class LogSamplerBucket  extends LogSampler {

        private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);
        private final Optional<LocalBucket> bucket;

        LogSamplerBucket(LoggingInterceptorConfig conf) {
            final Integer howOftenPerNSEconds = conf.getInt(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG);
            final Integer nSeconds = conf.getInt(LoggingInterceptorConfig.SAMPLE_RATE_BUCKET_SECONDS_CONFIG);
            Bandwidth limit;
            if (howOftenPerNSEconds == null || howOftenPerNSEconds < 0) {
                LOG.warn("Not rate limiting");
                this.bucket = Optional.empty();
            } else {
                limit = Bandwidth.simple(howOftenPerNSEconds, Duration.ofSeconds(nSeconds));
                this.bucket = Optional.ofNullable(Bucket.builder().addLimit(limit).build());
            }
        }

        @Override
        public boolean mark(String key) {
            return bucket.map(b -> b.tryConsume(1)).orElse(true);
        }
    }

    /**
     * Implementation based on random sampling
     */
    public static class LogSamplerRandom extends LogSampler {

        private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);
        private final double mark;

        LogSamplerRandom(LoggingInterceptorConfig conf) {
            Integer pct = conf.getInt(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG);
            if (pct == null || pct < 0 || pct >= 100) {
                LOG.warn("Not rate limiting");
                this.mark = 1.0;
            } else {
                this.mark = pct / 100.00;
            }
        }

        @Override
        public boolean mark(String key) {
            return Math.random() <= mark;
        }
    }

    /**
     *  Configuration enum
     */
    public enum SamplerType {
        Random("random"), TimeBucket("time-bucket");

        final String value;

        SamplerType(String value) {
            this.value = value;
        }

        public static SamplerType fromString(String c) {
            return Arrays.stream(values()).filter(t -> t.value.equalsIgnoreCase(c))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Can't convert: " + c));
        }

        public String getValue() {
            return value;
        }
    }

}
