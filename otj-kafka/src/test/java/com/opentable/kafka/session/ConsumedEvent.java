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
