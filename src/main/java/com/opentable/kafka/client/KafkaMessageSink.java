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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Throwables;
import com.google.common.base.Verify;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: this class probably wants to be smarter about thread safety.
// Initial synchronized implementation was observed to hold up fine in Buzzsaw
// at up to at least 80K msg/s but we can do better :)
public final class KafkaMessageSink<K, V> implements MessageSink<K, V, Callback> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageSink.class);
    /** Maximum buffer of futures we will wait for.  If this fills, forces a sync flush. */
    private static final long OUTSTANDING_PANIC = 1_000_000;

    @GuardedBy("this")
    private final LinkedHashSet<Future<RecordMetadata>> outstandingSends = new LinkedHashSet<>();

    private final Supplier<KafkaProducer<K, V>> supplier;
    private volatile KafkaProducer<K, V> producer;

    public static <K, V> KafkaMessageSink<K, V> of(Supplier<KafkaProducer<K, V>> supplier) {
        return new KafkaMessageSink<>(supplier);
    }

    private KafkaMessageSink(Supplier<KafkaProducer<K, V>> supplier) {
        this.supplier = supplier;
    }

    @Override
    public synchronized void open() {
        producer = supplier.get();
    }

    @Override
    public synchronized void close() {
        producer.close();
        producer = null;
    }

    /**
     * Flush outstanding messages.  If we fail to do so, throw an exception
     * and {@link #reset()} any other batched but uncommited messages.  The caller
     * should rewind and retry delivery.
     */
    @Override
    public synchronized void flush() {
        try {
            producer.flush();
            for (Future<RecordMetadata> f : outstandingSends) {
                Verify.verify(f.isDone(), "kafka promised future would be done after flush()");
                throwIfFailed(f);
            }
        } finally {
            reset();
        }
    }

    /** Forget any outstanding writes in the current batch.  Used when reinitializing after an error. */
    @Override
    public synchronized void reset() {
        outstandingSends.clear();
    }

    @Override
    public void send(String topic, K key, V message, Callback callback) {
        final Future<RecordMetadata> result =
            producer.send(
                new ProducerRecord<K, V>(topic, key, message),
                callback);

        trackResult(result);
    }

    private synchronized void trackResult(Future<RecordMetadata> result) {
        // Put future at tail
        outstandingSends.add(result);

        // Trim head of queue -- since delivery is in-order, so should acks
        // If somehow we get ack'ed out of order that's okay, next flushSync call will fix it
        final Iterator<Future<RecordMetadata>> iter = outstandingSends.iterator();
        while (iter.hasNext()) {
            Future<?> head = iter.next();
            if (head.isDone()) {
                iter.remove();
                throwIfFailed(head);
            } else {
                break;
            }
        }

        int size = outstandingSends.size();
        if (size > OUTSTANDING_PANIC) {
            // crazy -- did log rates go up?  or do we actually have an hours' worth of messages?
            LOG.error("{} messages have been queued but not awaited, forcing sync flush", size);
            flush();
        }
    }

    /** Throw if a future failed, otherwise discard result. */
    private void throwIfFailed(Future<?> f) {
        try {
            f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e);
        }
    }
}
