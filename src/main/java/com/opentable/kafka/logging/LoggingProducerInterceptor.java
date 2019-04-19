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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.util.LogSamplerRandom;

public class LoggingProducerInterceptor implements ProducerInterceptor<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);

    private String interceptorClientId;
    private LoggingUtils loggingUtils;
    private LogSamplerRandom sampler;

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        loggingUtils.setupHeaders(record);
        loggingUtils.setupTracing(sampler, record);
        loggingUtils.trace(LOG, interceptorClientId, record);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {
        //LOG.info("metadata: {}", metadata);
        if (e != null) {
            LOG.error("Error occurred during acknowledgement {}", metadata, e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        final LoggingInterceptorConfig conf = new LoggingInterceptorConfig(config);
        this.sampler = new LogSamplerRandom(conf.getDouble(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG));
        final String originalsClientId = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
        loggingUtils = (LoggingUtils) config.get("opentable.logging");
        //MJB: Dmitry why is this done?
        this.interceptorClientId = (originalsClientId == null) ? "interceptor-producer-" + ClientIdGenerator.INSTANCE.nextClientId() : originalsClientId;
        LOG.info("LoggingProducerInterceptor is configured for client: {}", interceptorClientId);
    }

    private static class ClientIdGenerator {
        public static ClientIdGenerator INSTANCE = new ClientIdGenerator();
        private final AtomicInteger IDS = new AtomicInteger(0);
        int nextClientId() {
            return IDS.getAndIncrement();
        }
    }

}
