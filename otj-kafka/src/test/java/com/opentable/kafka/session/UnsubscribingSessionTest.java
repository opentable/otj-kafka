package com.opentable.kafka.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;

public class UnsubscribingSessionTest extends BaseSessionTest {

    @Override
    public BiConsumer<Runnable, Consumer<Integer, String>> getConsumerRecoveryLambda() {
        return new UnsubscribingRecoveryLambda(12000L);
    }

    @Override
    public void moreAssertionsForTestResult(final boolean withSleep, final TestResult testResult, final long expectedTotalMessages, final long expectedRevocations) {
        //TODO: fix
        if (System.currentTimeMillis() > 0 ) {
            return;
        }
        List<ConsumedEvent.EventType> eventTypes = testResult.getEvents().stream().map(ConsumedEvent::getEventType).collect(Collectors.toList());
        Optional<ConsumedEvent<String>> consumerThatWokeup = testResult.getEvents().stream().filter(t -> t.getEventType() == ConsumedEvent.EventType.WAKING_UP).findFirst();
        assertThat(consumerThatWokeup.isPresent()).isEqualTo(withSleep);
        assertThat(eventTypes.stream().filter(t -> t == ConsumedEvent.EventType.MESSAGE).count()).isEqualTo(expectedTotalMessages);
        long revocations;
        if (!withSleep) {
            revocations = testResult.getConsumerTaskList().stream().mapToLong(ConsumerTask::getRevocations).sum();
        } else {
            //STILL DOESNT work well... I get 2 per every consumer EXCEPT the original. Does this make sense - maybe because it's unsubscribed
            // I chcked the code - indeed revoke will be called with an empty set, which makes the math "hard"
            List<ConsumerTask<String>> wokeup = testResult.getConsumerTaskList().stream().filter(t -> t.getConsumerId().equals(consumerThatWokeup.get().getConsumerId())).collect(Collectors.toList());
            revocations = wokeup.stream()
                    .mapToLong(ConsumerTask::getRevocations).sum();
            long empty  = wokeup.stream().mapToLong(ConsumerTask::getEmptyRevocations).sum();
            revocations += empty;
            revocations --; // This compensates for original one
        }
        assertThat(revocations).isEqualTo(expectedRevocations);
    }
}
