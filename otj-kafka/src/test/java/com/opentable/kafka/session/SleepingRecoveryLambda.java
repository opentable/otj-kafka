package com.opentable.kafka.session;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepingRecoveryLambda implements BiConsumer<Runnable, Consumer<Integer, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(SleepingRecoveryLambda.class);

    private final long sleepMillis;

    public SleepingRecoveryLambda(final long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void accept(final Runnable noop, final Consumer<Integer, String> kafkaConsumer) {
        LOG.info("Sleeping for {} ms, rebalance should occur...", sleepMillis);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
