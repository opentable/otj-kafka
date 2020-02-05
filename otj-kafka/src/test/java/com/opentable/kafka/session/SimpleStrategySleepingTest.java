package com.opentable.kafka.session;

import java.util.concurrent.ExecutionException;

import org.junit.Test;


public class SimpleStrategySleepingTest extends AbstractSleepingTest {

    // Shows a simple one consumer processes as expected, even after losing partitions
    // These have single producer/consumer, with bounded message count
    @Test(timeout = 35000L)
    public void testSimpleStrategy() throws ExecutionException, InterruptedException {
        performTestSimpleWithStategy();
    }

}
