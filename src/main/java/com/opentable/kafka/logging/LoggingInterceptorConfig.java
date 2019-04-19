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

public class LoggingInterceptorConfig extends AbstractConfig {

    public static final Double DEFAULT_SAMPLE_RATE_PCT = .1;
    public static final String SAMPLE_RATE_PCT_CONFIG = "logging.ot.sample-rate";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(SAMPLE_RATE_PCT_CONFIG, Type.DOUBLE, DEFAULT_SAMPLE_RATE_PCT, ConfigDef.Importance.LOW,
            "Logging sample rate.");

    public LoggingInterceptorConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}
