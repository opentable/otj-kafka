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
package com.opentable.kafka.logging;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingConsumerInterceptor.class);
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private String originalsClientId;
    private String interceptorClientId;

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        this.originalsClientId = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
        String interceptorClientId = (originalsClientId == null) ? "interceptor-consumer-" + ClientIdGenerator.nextClientId() : originalsClientId;
        LOG.info("LoggingProducerInterceptor is configured for client: {}", interceptorClientId);
    }

    private static class ClientIdGenerator {
        private static final AtomicInteger IDS = new AtomicInteger(0);
        static int nextClientId() {
            return IDS.getAndIncrement();
        }
    }
}
