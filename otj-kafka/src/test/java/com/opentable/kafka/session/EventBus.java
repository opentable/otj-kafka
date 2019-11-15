package com.opentable.kafka.session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This merely collects the events...
 * @param <T>
 */
public class EventBus<T> {
    public static interface Listener<T> {
        void listen(ConsumedEvent<T> event);
    }

    private final List<Listener<T>> listeners = new CopyOnWriteArrayList<>();
    private final List<ConsumedEvent<T>> events = new ArrayList<>();

    public void send(ConsumedEvent<T> event) {
        synchronized (events) {
            events.add(event);
            listeners.forEach(t -> t.listen(event));
        }
    }

    public void addListener(Listener<T> listener) {
        listeners.add(listener);
    }

    public List<ConsumedEvent<T>> getEvents() {
        synchronized(events) {
            return new ArrayList<>(events);
        }
    }
}
