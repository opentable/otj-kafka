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

import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.util.ClientIdGenerator;

/**
 * Logging interceptor to add otl based logging
 * @param <K> key
 * @param <V> value
 */
public class LoggingConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingConsumerInterceptor.class);

    // See configure()
    private String interceptorClientId;
    private String groupId;
    private LoggingUtils loggingUtils;
    private LogSampler sampler;

    public LoggingConsumerInterceptor() { //NOPMD
        /* noargs needed for kafka */
    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(record -> loggingUtils.maybeLogConsumer(LOG, interceptorClientId, groupId, sampler, record));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        /* no op */
    }

    @Override
    public void close() {
        LOG.debug("Shutting down LoggingConsumerInterceptor");
    }

    @VisibleForTesting
    LoggingUtils getLoggingUtils(Map<String, ?> config) {
        return new LoggingUtils((EnvironmentProvider) config.get(LoggingInterceptorConfig.LOGGING_ENV_REF));
    }

    @Override
    public void configure(Map<String, ?> config) {
        final LoggingInterceptorConfig conf = new LoggingInterceptorConfig(config);
        String originalsClientId = (String) config.get(ConsumerConfig.CLIENT_ID_CONFIG);
        groupId  = (String) config.get(ConsumerConfig.GROUP_ID_CONFIG);
        loggingUtils = getLoggingUtils(config);
        sampler = LogSampler.create(conf);
        interceptorClientId = (originalsClientId == null) ? "interceptor-consumer-" +  ClientIdGenerator.getInstance().nextConsumerId() : originalsClientId;
        LOG.info("LoggingConsumerInterceptor is configured for client: {}, group-id: {}", interceptorClientId, groupId);
    }
}
