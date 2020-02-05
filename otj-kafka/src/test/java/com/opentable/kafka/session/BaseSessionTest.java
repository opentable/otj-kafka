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
package com.opentable.kafka.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

// Actual base tests are here
public abstract class BaseSessionTest extends BaseSessionScaffold {
    public abstract BiConsumer<Runnable, Consumer<Integer, String>> getConsumerRecoveryLambda();
    public abstract void moreAssertionsForTestResult(final boolean withSleep, final TestResult testResult, final long expectedTotalMessages, final long expectedRevocations);
    // Shows a simple one consumer processes as expected.
    // These have single producer/consumer, with bounded message count
    public void performTestSimpleNoStrategy() throws ExecutionException, InterruptedException {
        final int totalMessages = 100;
        final TestResult simpleTestResult = commonSimpleTests(totalMessages, totalMessages * 10);
        final List<ConsumedEvent.EventType> eventTypes = commonSimpleAssertions(false, simpleTestResult, totalMessages);
        assertThat(eventTypes).doesNotContain(ConsumedEvent.EventType.ABOUT_TO_SLEEP, ConsumedEvent.EventType.WAKING_UP);
    }

    // Shows a simple one consumer processes as expected, even after losing partitions
    // These have single producer/consumer, with bounded message count
    public void performTestSimpleWithStategy() throws ExecutionException, InterruptedException {
        final TestResult simpleTestResult = commonSimpleTests(100, 20);
        // Note the batch of 20 gets repeated.
        final List<ConsumedEvent.EventType> eventTypes = commonSimpleAssertions(true, simpleTestResult, 100 + 20);
        assertThat(eventTypes).containsOnlyOnce(ConsumedEvent.EventType.ABOUT_TO_SLEEP, ConsumedEvent.EventType.WAKING_UP);
    }

    private List<ConsumedEvent.EventType> commonSimpleAssertions(boolean withSleep, TestResult simpleTestResult, int totalMessages) {
        List<ConsumedEvent.EventType> eventTypes = simpleTestResult.getEvents().stream().map(ConsumedEvent::getEventType).collect(Collectors.toList());
        // Once per consumer task
        LOG.info("EventTypes: \n{}", new HashSet<>(eventTypes));
        assertThat(eventTypes).containsOnlyOnce(ConsumedEvent.EventType.SUBSCRIBED, ConsumedEvent.EventType.CLOSING);
        assertThat(eventTypes.stream().filter(t -> t == ConsumedEvent.EventType.POLL).count()).isGreaterThanOrEqualTo(totalMessages);

        // Has consumed a total of N messages
        assertThat(eventTypes.stream().filter(t -> t == ConsumedEvent.EventType.POLL).count()).isGreaterThanOrEqualTo(totalMessages);
        moreAssertionsForTestResult(withSleep, simpleTestResult, (long) totalMessages, withSleep ? 1L : 0L);
        return eventTypes;
    }

    private TestResult commonSimpleTests(int totalMessages, int sleepCount) throws ExecutionException, InterruptedException {
        final Producer<Integer, String> producer = producer();
        final Consumer<Integer, String> consumer = consumer(1, UUID.randomUUID().toString());
        EventBus.Listener<String> listener = new SimpleBoundedListener<String>(totalMessages, consumer);
        eventBus.addListener(listener);

        final ProducerTask producerTask = getProducerTask(producer, totalMessages);
        final ConsumerTask<String> consumerTask = getConsumerTask(consumer, sleepCount);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> producerFuture =  executorService.submit(producerTask);
        Future<?> consumerFuture = executorService.submit(consumerTask);
        // We can simply complete since producer is bounded
        producerFuture.get();
        LOG.info("Producer future completed");
        // The consumer cascades from the BoundedListener
        consumerFuture.get();
        executorService.shutdownNow();
        List<ConsumedEvent<String>> events = eventBus.getEvents().stream().sorted(Comparator.comparing(ConsumedEvent::getMessageNumber)).collect(Collectors.toList());
        LOG.warn("Events: {}", events.size());
        events.forEach(t -> LOG.info(t.toString()));

        return new TestResult(Collections.singletonList(consumerTask), events, producerTask);
    }

    // Same as the simple test, but with 3 consumers
    // Total messages remains bounded, and no sleep, so under normal circumstances no rebalance
    public void performComplexTestWithNoStrategy() throws ExecutionException, InterruptedException {
        final int totalMessages = 100;
        TestResult complexTestResult = commonComplexTestsWithSleep(totalMessages, 110000, 3);
        List<ConsumedEvent<String>> events = complexTestResult.getEvents();
        assertThat(complexTestResult.getProducerTask().getTotalMessages()).isEqualTo(totalMessages);
        Set<ConsumedEvent.EventType> eventTypeList = events.stream().map(ConsumedEvent::getEventType).collect(Collectors.toSet());
        List<ConsumedEvent<String>> messageEvents = events.stream().filter(t -> t.getEventType() == ConsumedEvent.EventType.MESSAGE).sorted(Comparator.comparing(t -> t.getMessageNumber())).collect(Collectors.toList());
        Set<UUID> consumerIds = messageEvents.stream().map(ConsumedEvent::getConsumerId).collect(Collectors.toSet());
        assertThat(consumerIds).hasSize(3);
        assertThat(eventTypeList).containsOnly(ConsumedEvent.EventType.SUBSCRIBED, ConsumedEvent.EventType.MESSAGE, ConsumedEvent.EventType.CLOSING, ConsumedEvent.EventType.POLL);
        moreAssertionsForTestResult(false, complexTestResult,100L,0L);
    }

    // Unbounded producer (see ComplexPotentiallyUnBoundedListener for the cascaded logic that causes a termination)
    // multiple consumers
    // One consumer will sleep beyond session.timeout.ms
    public void performComplexTestWithStrategy() throws ExecutionException, InterruptedException {
        // We don't bound the totalMessages, instead we rely on the complex cascade in ComplexBoundedListener.
        Integer initialTotalMessages = null;
        // Sleep at item # 50 consumed for consumer #1. Create 3 consumers
        TestResult complexTestResult = commonComplexTestsWithSleep(initialTotalMessages, 50, 3);
        List<ConsumedEvent<String>> events = complexTestResult.getEvents();
        int totalMessages = complexTestResult.getProducerTask().getTotalMessages() + 50;
        Set<ConsumedEvent.EventType> eventTypeList = events.stream().map(ConsumedEvent::getEventType).collect(Collectors.toSet());
        List<ConsumedEvent<String>> messageEvents = events.stream().filter(t -> t.getEventType() == ConsumedEvent.EventType.MESSAGE).sorted(Comparator.comparing(t -> t.getMessageNumber())).collect(Collectors.toList());
        Set<UUID> consumerIds = messageEvents.stream().map(ConsumedEvent::getConsumerId).collect(Collectors.toSet());

        // All the events we expected
        assertThat(eventTypeList).contains(ConsumedEvent.EventType.POLL, ConsumedEvent.EventType.CLOSING, ConsumedEvent.EventType.SUBSCRIBED, ConsumedEvent.EventType.WAKING_UP, ConsumedEvent.EventType.ABOUT_TO_SLEEP);
       // assertThat(messageEvents).hasSize(totalMessages);
        assertThat(consumerIds).hasSize(3);

        // Prove we continued to get messages
        int countOfAboutToPause = 0;
        int indexOfAboutToWakeup = -1;
        int countOfAboutToWakeup = 0;
        UUID consumerIdWakingUp = null;
        for (int i =0; i< events.size() ; i++) {
            ConsumedEvent<String> event = events.get(i);
            switch (event.getEventType()) {
                case ABOUT_TO_SLEEP: {
                    countOfAboutToPause++;
                    break;
                }
                case WAKING_UP: {
                    if (countOfAboutToPause > 0) {
                        countOfAboutToWakeup++;
                        consumerIdWakingUp = event.getConsumerId();
                        indexOfAboutToWakeup = i;
                    }
                    break;
                }
            }
        }
        assertThat(countOfAboutToPause).isEqualTo(1);
        assertThat(countOfAboutToWakeup).isEqualTo(1);
        assertThat(consumerIdWakingUp).isNotNull();
        assertThat(indexOfAboutToWakeup).isGreaterThan(-1);
        List<ConsumedEvent<String>> eventsAfterIndex = events.subList(indexOfAboutToWakeup + 1, events.size());
        final UUID c = consumerIdWakingUp;
        List<ConsumedEvent<String>> messageEventsAfterWakingUp = eventsAfterIndex.stream().filter(t -> t.getConsumerId().equals(c)).filter(t -> t.getEventType() == ConsumedEvent.EventType.MESSAGE).collect(Collectors.toList());
        assertThat(messageEventsAfterWakingUp.size()).isGreaterThan(0);
        moreAssertionsForTestResult(true, complexTestResult, (long) totalMessages, 1L);
 }

    private TestResult commonComplexTestsWithSleep(Integer totalMessages, int sleepCount, int consumerCount) throws ExecutionException, InterruptedException {
        final Producer<Integer, String> producer = producer();
        final List<Consumer<Integer, String>> consumerList = new ArrayList<>();
        final UUID groupId = UUID.randomUUID();

        // Create consumerCount total Consumers
        for (int i = 1; i <= consumerCount; i++) {
            final Consumer<Integer, String> consumer = consumer(i, groupId.toString());
            consumerList.add(consumer);
        }

        final ProducerTask producerTask = getProducerTask(producer, totalMessages);
        final EventBus.Listener<String> listener = new ComplexPotentiallyUnBoundedListener<>(
                totalMessages, consumerList, producerTask);
        eventBus.addListener(listener);

        // Create consumerCount total consumerTasks
        final List<ConsumerTask<String>> consumerTaskList = new ArrayList<>();
        for (int i = 1; i <= consumerCount; i++) {
            // Only attach to one of the consumers, the others will pound away
            if (i == 1) {
                final ConsumerTask<String> consumerTask = getConsumerTask(
                        consumerList.get(i - 1), sleepCount);
                consumerTaskList.add(consumerTask);
                continue;
            }
            // These will either be set higher than bound (if bounded)
            // or null, which disables sleeping altogether
            // Net effect is only the i==1 consumer is eligible for sleeping
            final ConsumerTask<String> consumerTask = getConsumerTask(
                    consumerList.get(i - 1), totalMessages == null ? null : totalMessages * 10);
            consumerTaskList.add(consumerTask);
        }
        assertThat(consumerTaskList).hasSize(consumerCount);

        // Allocate consumerCount + 1 producer thread.
        final ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        Future<?> producerFuture =  executorService.submit(producerTask);

        // Create consumerCount total futures....
        List<Future<?>> consumerFutureList = new ArrayList<>();
        for (int i = 1; i <= consumerCount; i++) {
            Future<?> consumerFuture = executorService.submit(consumerTaskList.get(i -1));
            consumerFutureList.add(consumerFuture);
        }

        // We can simply complete since producer is bounded or cascades from ComplexBoundedListener
        producerFuture.get();
        LOG.info("Producer future completed");
        // The consumer cascades from the ComplexBoundedListener
        for (int i = 1; i< consumerCount; i++) {
            consumerFutureList.get(i - 1).get();
        }
        executorService.shutdownNow();
        List<ConsumedEvent<String>> events = eventBus.getEvents();
        LOG.info("Events: {} ",  events);
        return new TestResult(consumerTaskList, events, producerTask);
    }

}
