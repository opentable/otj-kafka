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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

import com.opentable.kafka.logging.LogSampler.SamplerType;

public class LoggingInterceptorConfig extends AbstractConfig {

    // Key used to store an object reference to EnvironmentProvider
    public static final String LOGGING_ENV_REF = "ot.logging.reference";
    // Key used to store the limit rate
    public static final String SAMPLE_RATE_PCT_CONFIG = "ot.logging.rate";
    public static final int DEFAULT_SAMPLE_RATE_PCT = 1;
    // Key used to store the sampler type
    public static final String SAMPLE_RATE_TYPE_CONFIG = "ot.logging.sampler_type";
    public static final String DEFAULT_SAMPLE_RATE_TYPE = SamplerType.TimeBucket.value;

    // Whether to stop header propagation
    public static final String ENABLE_HEADER_PROPAGATION_CONFIG = "ot.logging.headers";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(SAMPLE_RATE_PCT_CONFIG, Type.INT, DEFAULT_SAMPLE_RATE_PCT, ConfigDef.Importance.LOW,
            "Logging limit rate per 10 seconds for time-bucket or percent of records for random sampler. Use a negative value to disable limiting (lots of logs!) ")
        .define(SAMPLE_RATE_TYPE_CONFIG, Type.STRING, DEFAULT_SAMPLE_RATE_TYPE, ConfigDef.Importance.LOW,
            "Logging sampler type. Possible values: (random, time-bucket)")
         .define(ENABLE_HEADER_PROPAGATION_CONFIG, Type.STRING, GenerateHeaders.ALL.name(),
                 ConfigDef.Importance.LOW, "Whether to use headers for propagation");

    LoggingInterceptorConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}
