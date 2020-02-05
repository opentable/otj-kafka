package com.opentable.kafka.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;


public abstract class AbstractSleepingTest extends BaseSessionTest {

    @Override
    public BiConsumer<Runnable, Consumer<Integer, String>> getConsumerRecoveryLambda() {
        return new SleepingRecoveryLambda(12000);
    }

    @Override
    public void moreAssertionsForTestResult(final boolean withSleep, final TestResult testResult, final long expectedTotalMessages, final long expectedRevocations) {
        List<ConsumedEvent.EventType> eventTypes = testResult.getEvents().stream().map(ConsumedEvent::getEventType).collect(Collectors.toList());
        Optional<ConsumedEvent<String>> consumerThatWokeup = testResult.getEvents().stream().filter(t -> t.getEventType() == ConsumedEvent.EventType.WAKING_UP).findFirst();
        assertThat(consumerThatWokeup.isPresent()).isEqualTo(withSleep);
        assertThat(eventTypes.stream().filter(t -> t == ConsumedEvent.EventType.MESSAGE).count()).isEqualTo(expectedTotalMessages);
        long revocations = 0;
        if (!withSleep) {
            revocations = testResult.getConsumerTaskList().stream().mapToLong(ConsumerTask::getRevocations).sum();
        } else {
            revocations = testResult.getConsumerTaskList().stream().filter(t -> t.getConsumerId().equals(consumerThatWokeup.get().getConsumerId())).collect(Collectors.toList()).stream()
                    .mapToLong(ConsumerTask::getRevocations).sum();
        }
        assertThat(revocations).isEqualTo(expectedRevocations);
    }
}
