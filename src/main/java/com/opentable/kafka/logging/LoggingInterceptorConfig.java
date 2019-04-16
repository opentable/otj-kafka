package com.opentable.kafka.logging;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

public class LoggingInterceptorConfig extends AbstractConfig {

    public static final String DEFAULT_SAMPLE_RATE_PCT = "1.0";
    public static final String SAMPLE_RATE_PCT_CONFIG = "logging.ot.sample-rate";

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(SAMPLE_RATE_PCT_CONFIG, Type.DOUBLE, DEFAULT_SAMPLE_RATE_PCT, ConfigDef.Importance.LOW,
            "Logging sample rate.");

    public LoggingInterceptorConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}
