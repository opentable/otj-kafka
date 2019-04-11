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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.util.LogSamplerRandom;

public class LoggingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);

    private String interceptorClientId;
    private LogSamplerRandom sampler = new LogSamplerRandom(5.0);

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        LoggingUtils.setupHeaders(record);
        LoggingUtils.setupTracing(sampler, record);
        LoggingUtils.trace(LOG, interceptorClientId, record);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {
        //LOG.info("metadata: {}", metadata);
        if (e != null) {
            LOG.error("", e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        String originalsClientId = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
        this.interceptorClientId = (originalsClientId == null) ? "interceptor-producer-" + ClientIdGenerator.nextClientId() : originalsClientId;
        LOG.info("LoggingProducerInterceptor is configured for client: {}", interceptorClientId);
    }

    private static class ClientIdGenerator {
        private static final AtomicInteger IDS = new AtomicInteger(0);
        static int nextClientId() {
            return IDS.getAndIncrement();
        }
    }

}
