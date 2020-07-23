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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTask<T> implements Runnable {
    private static final Logger LOG  = LoggerFactory.getLogger(ConsumerTask.class);

    private final Consumer<Integer, T> consumer;
    private final String topic;
    private final AtomicInteger consumedEvents = new AtomicInteger();
    private volatile boolean running = true;
    private final EventBus<T> eventBus;
    private final Integer sleepPoint;
    private final UUID consumerId = UUID.randomUUID();
    private final BiConsumer<Runnable, Consumer<Integer, T>> recoveryLambda;

    private final AtomicInteger revocations = new AtomicInteger();
    private final AtomicInteger emptyRevocations = new AtomicInteger();


    public ConsumerTask(final EventBus<T> eventBus, final Consumer<Integer, T> consumer, final String topic,
                        final Integer sleepPoint, final BiConsumer<Runnable, Consumer<Integer, T>> recoveryLambda) {
        this.consumer = consumer;
        this.topic = topic;
        this.sleepPoint = sleepPoint;
        this.eventBus = eventBus;
        this.recoveryLambda = recoveryLambda;
        subscribe();
        eventBus.send(new ConsumedEvent<>(consumerId, null, ConsumedEvent.EventType.SUBSCRIBED, -1, -1, -1));
    }

    public int getEmptyRevocations() {
        return emptyRevocations.get();
    }

    private void subscribe() {
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    revocations.incrementAndGet();
                    LOG.warn("OTPARTITION Revoke ({}) - {}", consumerId, partitions);
                } else {
                    emptyRevocations.incrementAndGet();
                }
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    LOG.warn("OTPARTITION Assign ({}) - {}", consumerId, partitions);
                }
            }
        });
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }

    @Override
    public void run() {
        try {
            while (getMayRun()) {
                tick();
            }
        } catch (WakeupException e) {
            running = false;
        } finally {
            eventBus.send(new ConsumedEvent<>(consumerId, null, ConsumedEvent.EventType.CLOSING, consumedEvents.get(), -1, -1));
            consumer.close();
        }
    }

    private void tick() {
        eventBus.send(new ConsumedEvent<>(consumerId, null, ConsumedEvent.EventType.POLL, consumedEvents.get(), -1, -1));
        // shows this idiom is safe even on freshly subscribed
        consumer.resume(consumer.paused());
        ConsumerRecords<Integer, T> records = consumer.poll(Duration.ofMillis(500));
        if (!records.isEmpty()) {
            // Again, mostly proving this no-op works
            consumer.pause(consumer.assignment());
            process(records);
        }
    }

    private void process(final ConsumerRecords<Integer, T> records) {
        // Because max Poll records == 1, this should only return 1
        if (records.count() != 1) {
            throw new IllegalStateException("Only 1");
        }
        for (ConsumerRecord<Integer, T> record : records) {
            if (getMayRun()) {
                int howManyRecordsIveConsumed = consumedEvents.incrementAndGet();
                LOG.info("{} processed {} records", consumerId, howManyRecordsIveConsumed);
                eventBus.send(new ConsumedEvent<>(consumerId, record.value(), ConsumedEvent.EventType.MESSAGE, record.key(), record.partition(), record.offset()));

                if (sleepPoint != null && howManyRecordsIveConsumed == sleepPoint) {
                    eventBus.send(new ConsumedEvent<>(consumerId, record.value(), ConsumedEvent.EventType.ABOUT_TO_SLEEP, record.key(), record.partition(), record.offset()));
                    LOG.info("Calling sleep point");
                    this.recoveryLambda.accept(this::subscribe, consumer);
                    eventBus.send(new ConsumedEvent<>(consumerId, record.value(), ConsumedEvent.EventType.WAKING_UP, record.key(), record.partition(), record.offset()));
                }
                break;
            }
        }
    }

    public UUID getConsumerId() {
        return consumerId;
    }

    public int getRevocations() {
        return revocations.get();
    }

    private boolean getMayRun() {
        return running && !Thread.currentThread().isInterrupted();
    }
}
