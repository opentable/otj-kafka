package com.opentable.kafka.session;

import java.util.concurrent.ExecutionException;

import org.junit.Test;


public class ComplexNoStrategyUnsubscribingTest extends AbstractUnsubscribingTest {


    // Same as the simple test, but with 3 consumers
    // Total messages remains bounded, and no sleep, so under normal circumstances no rebalance
    @Test(timeout = 15000)
    public void testComplexNoStrategy() throws ExecutionException, InterruptedException {
        performComplexTestWithNoStrategy();
    }

}
