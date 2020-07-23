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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A more complex algorithm that assumes
 * - only Zero/one wakeup
 * - either a nonnull totalMessages in constructor
 * - OR a WAKEUP occures
 * - For a wakeup, this triggers a watch for a message
 * from same consumer, which in turn triggers a shut down of the producer.
 * which in turn waits for the remainder to be consumed.
 * @param <T>
 */
public class ComplexPotentiallyUnBoundedListener<T> implements EventBus.Listener<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ComplexPotentiallyUnBoundedListener.class);
    private volatile Integer totalMessages;
    private final List<Consumer<Integer, String>> consumers;
    private final Set<Integer> processedMessages = ConcurrentHashMap.newKeySet();
    private final AtomicReference<UUID> consumerIdThatWokeup = new AtomicReference<>();
    private final ProducerTask producerTask;

    public ComplexPotentiallyUnBoundedListener(Integer totalMessages, List<Consumer<Integer, String>> consumers, ProducerTask producerTask) {
        this.totalMessages = totalMessages;
        this.consumers = consumers;
        this.producerTask = producerTask;
    }
    @Override
    public synchronized void listen(final ConsumedEvent<T> event) {
        LOG.debug("listener: {}", event);
        int messageNumber = event.getMessageNumber();
        switch (event.getEventType()) {
            case MESSAGE: {
                if (messageNumber > 0) {
                    processedMessages.add(messageNumber);
                }
                if (!producerTask.isStopped() && (consumerIdThatWokeup.get() != null) && (consumerIdThatWokeup.get().equals(event.getConsumerId()))) {
                    LOG.debug("Signal producer to stop");
                    producerTask.stop();
                    waitForProducerToStop();
                    if (producerTask.isStopped()) {
                        totalMessages = producerTask.getTotalMessages();
                    }
                }
                break;
            }
            case POLL: {
                if ((totalMessages != null) && (processedMessages.size() >= totalMessages)) {
                    // we're done!
                    consumers.forEach(Consumer::wakeup);
                }
                break;
            }
            case WAKING_UP: {
                consumerIdThatWokeup.set(event.getConsumerId());

                break;
            }
/*
        The producer should finish:
            - Once we've woken up
            &&
           - There's a message from the same consumerId, which implies a rebalance occurred

           Then you must signal the producer.setTotalMessages to shutdown, and wait for it to finish
           Then the consumers can in turn be shut downn during POLL, if we've met the totalMessages


 */
        }
    }

    private void waitForProducerToStop() {
        // spin locking is evil, and I probably should do message passing instead
        while (!producerTask.isStopped()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
