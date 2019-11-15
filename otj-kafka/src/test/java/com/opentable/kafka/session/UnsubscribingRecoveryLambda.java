package com.opentable.kafka.session;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsubscribingRecoveryLambda implements BiConsumer<Runnable, Consumer<Integer, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(UnsubscribingRecoveryLambda.class);

    private final long sleepMillis;

    public UnsubscribingRecoveryLambda(final long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void accept(final Runnable subscriber, final Consumer<Integer, String> kafkaConsumer) {
        LOG.info("Unsubscribing for [} ms, rebalance should occur...", sleepMillis);
        try {
            kafkaConsumer.unsubscribe();
            Thread.sleep(sleepMillis);
            subscriber.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
