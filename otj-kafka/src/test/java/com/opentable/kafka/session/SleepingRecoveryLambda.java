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

import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepingRecoveryLambda implements BiConsumer<Runnable, Consumer<Integer, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(SleepingRecoveryLambda.class);

    private final long sleepMillis;

    public SleepingRecoveryLambda(final long sleepMillis) {
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void accept(final Runnable noop, final Consumer<Integer, String> kafkaConsumer) {
        LOG.info("Sleeping for {} ms, rebalance should occur...", sleepMillis);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
