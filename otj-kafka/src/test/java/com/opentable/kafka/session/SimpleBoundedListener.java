
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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assumes that a final message count is sufficient to trigger the shutdown
 * @param <T>
 */
public class SimpleBoundedListener<T> implements EventBus.Listener<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleBoundedListener.class);

    private volatile Integer totalMessages;
    private final List<Consumer<Integer, String>> consumers;
    private final Set<Integer> processedMessages = ConcurrentHashMap.newKeySet();

    public SimpleBoundedListener(Integer totalMessages, Consumer<Integer, String> consumers) {
        this(totalMessages, Collections.singletonList(consumers));
    }
    public SimpleBoundedListener(Integer totalMessages, List<Consumer<Integer, String>> consumers) {
        this.totalMessages = totalMessages;
        this.consumers = consumers;
    }
    @Override
    public void listen(final ConsumedEvent<T> event) {
        LOG.debug("listener: {} ",  event);
        int messageNumber = event.getMessageNumber();
        switch (event.getEventType()) {
            case MESSAGE: {
                if (messageNumber > 0) {
                    processedMessages.add(messageNumber);
                }
                break;
            }
            case POLL: {
                if ((totalMessages != null) && (processedMessages.size() == totalMessages)) {
                    // we're done!
                    consumers.forEach(Consumer::wakeup);
                }
                break;
            }
        }
    }
}
