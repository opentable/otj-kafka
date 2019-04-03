package com.opentable.kafka.util;

public class LogSamplerRandom {

    private final double mark;

    public LogSamplerRandom(double pct) {
        this.mark = pct / 100.00;
    }

    public boolean mark(String key) {
        return Math.random() < mark;
    }

}
