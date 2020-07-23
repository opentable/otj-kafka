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
