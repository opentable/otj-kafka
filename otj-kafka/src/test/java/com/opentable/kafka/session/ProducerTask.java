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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerTask implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(ProducerTask.class);
    final private Producer<Integer, String> producer;
    final private String topic;
    final private AtomicInteger producedMessages = new AtomicInteger();
    private volatile boolean running = true;
    private volatile boolean isStopped = false;
    private volatile Integer maxRecords;

    public ProducerTask(Producer<Integer, String> producer, String topic, Integer maxRecords) {
        this.producer = producer;
        this.maxRecords = maxRecords;
        this.topic = topic;
    }

    @Override
    public void run() {
        try {
            while (mayRun()) {
                produce();
            }
        } finally {
            producer.close(Duration.ofMinutes(1));
            isStopped = true;
        }

        LOG.info("Producer shutting down");
    }

    public void stop() {
        running = false;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public void setMaxRecords(final Integer maxRecords) {
        this.maxRecords = maxRecords;
    }

    private void produce() {
        try {
            long produced = producedMessages.incrementAndGet();
            // synchronous send
            producer.send(new ProducerRecord<>(topic, (int) produced, "produced" + produced)).get();
            // Besides mayRun() - which could be called externally or via future.cancel, this terminates as follows
            // If maxRecords is set, then we'll stop at >= maxRecords
            if ((maxRecords != null) && (maxRecords > 0) && (produced >= maxRecords)) {
                LOG.info("Stopping after producing {}", produced);
                running = false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private boolean mayRun() {
        return running && !Thread.currentThread().isInterrupted();
    }

    public int getTotalMessages() {
        return producedMessages.get();
    }
}
