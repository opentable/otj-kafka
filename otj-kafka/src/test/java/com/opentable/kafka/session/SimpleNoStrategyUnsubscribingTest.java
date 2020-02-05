package com.opentable.kafka.session;

import java.util.concurrent.ExecutionException;

import org.junit.Test;


public class SimpleNoStrategyUnsubscribingTest extends AbstractUnsubscribingTest {
    // Shows a simple one consumer processes as expected.
    // These have single producer/consumer, with bounded message count
    @Test(timeout = 15000L)
    public void testSimpleNoStrategy() throws ExecutionException, InterruptedException {
        performTestSimpleNoStrategy();
    }

}
