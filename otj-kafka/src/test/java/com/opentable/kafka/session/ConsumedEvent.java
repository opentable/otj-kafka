
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

import java.util.UUID;

// Each event consumed by EventBus or emitted as listener
public class ConsumedEvent<T> {

    private final UUID consumerId;
    private final int messageNumber;
    private final T message;
    private final EventType eventType;
    private final int partition;
    private final long offset;

    public ConsumedEvent(final UUID consumerId, final T message, final EventType eventType, int messageNumber, int partition, long offset) {
        this.consumerId = consumerId;
        this.message = message;
        this.eventType = eventType;
        this.offset = offset;
        this.partition = partition;
        this.messageNumber = messageNumber;
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public EventType getEventType() {
        return eventType;
    }

    public T getMessage() {
        return message;
    }

    public UUID getConsumerId() {
        return consumerId;
    }

    public enum EventType {
        SUBSCRIBED,
        MESSAGE,
        ABOUT_TO_SLEEP,
        WAKING_UP, POLL, CLOSING
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConsumedEvent{");
        sb.append("consumerId=").append(consumerId);
        sb.append(", messageNumber=").append(messageNumber);
        sb.append(", message=").append(message);
        sb.append(", eventType=").append(eventType);
        sb.append(", partition=").append(partition);
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }
}
