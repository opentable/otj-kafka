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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.util.LogSamplerRandom;

public class LoggingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingConsumerInterceptor.class);

    private String interceptorClientId;
    private String groupId;
    private volatile LoggingUtils loggingUtils;
    private LogSamplerRandom sampler;

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(record -> loggingUtils.trace(LOG, interceptorClientId, groupId, sampler, record));
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
        final LoggingInterceptorConfig conf = new LoggingInterceptorConfig(config);
        // Dmitry - consider using bucket4j instead
        this.sampler = new LogSamplerRandom(conf.getDouble(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG));
        String originalsClientId = (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG);
        groupId  = (String) config.get(ConsumerConfig.GROUP_ID_CONFIG);
        loggingUtils = (LoggingUtils) config.get(LoggingInterceptorConfig.LOGGING_REF);
        interceptorClientId = (originalsClientId == null) ? "interceptor-consumer-" + ClientIdGenerator.INSTANCE.nextClientId() : originalsClientId;
        LOG.info("LoggingConsumerInterceptor is configured for client: {}, group-id: {}", interceptorClientId, groupId);
    }

    private static class ClientIdGenerator {
        public static ClientIdGenerator INSTANCE = new ClientIdGenerator();
        private final AtomicInteger IDS = new AtomicInteger(0);
        int nextClientId() {
            return IDS.getAndIncrement();
        }
    }
}
