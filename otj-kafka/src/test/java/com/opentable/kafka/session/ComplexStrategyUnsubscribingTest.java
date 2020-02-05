package com.opentable.kafka.session;

import java.util.concurrent.ExecutionException;

import org.junit.Test;


public class ComplexStrategyUnsubscribingTest extends AbstractUnsubscribingTest {


    // Same as the simple test, but with 3 consumers
    // Total messages remains bounded, and no sleep, so under normal circumstances no rebalance
    @Test(timeout = 35000)
    public void testComplexWithStrategy() throws ExecutionException, InterruptedException {
        performComplexTestWithStrategy();
    }

}
