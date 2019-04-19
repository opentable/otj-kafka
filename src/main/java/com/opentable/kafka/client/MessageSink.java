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
package com.opentable.kafka.client;

import java.io.Closeable;

/**
 * Common interface that accepts messages.
 *
 * The destination is expected to be <b>idempotent</b>.
 * At any point, the destination may fail.  After reconnecting some messages
 * may be re-played.  This implements <b>at-least-once</b> delivery.
 *
 * This interface exists mostly for testability.
 */
public interface MessageSink<K, V, CallbackType> extends Closeable {

    /**
     * Connect to the destination and prepare for sending.
     */
    void open();

    /**
     * Flush outstanding messages synchronously.  If we fail to do so,
     * throw an exception and {@link reset} any other sent
     * but unacknowledged batches -- the caller is expected to rewind and retry.
     */
    void flush();

    /** Forget any outstanding writes.  Used when reinitializing after an error. */
    void reset();

    /**
     * Offer a message to the destination.  May throw delayed exceptions from any message
     * previously offered due to batching.
     * @param topic The kafka topic
     * @param key The key
     * @param message the value
     * @param callback callback
     */
    void send(String topic, K key, V message, CallbackType callback);
}
