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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.util.ClientIdGenerator;

/**
 * Intercepts all KafkaProducer traffic and applies otl-standardized logging and metrics to them
 */
public class LoggingProducerInterceptor implements ProducerInterceptor<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);

    // see configure()
    private String interceptorClientId;
    private LoggingUtils loggingUtils;
    private LogSampler sampler;


    public LoggingProducerInterceptor() { //NOPMD
        /* noargs needed for kafka */
    }

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        loggingUtils.addHeaders(record);
        loggingUtils.setTracingHeader(sampler, record);
        loggingUtils.maybeLogProducer(LOG, interceptorClientId, record);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {
        if (e != null) {
            LOG.error("Error occurred during acknowledgement {}", metadata, e);
        }
    }

    @Override
    public void close() {
        LOG.debug("Shutting down LoggingProducerInterceptor");
    }

    @VisibleForTesting
    LoggingUtils getLoggingUtils(Map<String, ?> config) {
        return new LoggingUtils((EnvironmentProvider) config.get(LoggingInterceptorConfig.LOGGING_ENV_REF));
    }

    @Override
    public void configure(Map<String, ?> config) {
        final LoggingInterceptorConfig conf = new LoggingInterceptorConfig(config);
        final String originalsClientId = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
        loggingUtils = getLoggingUtils(config);
        sampler = LogSampler.create(conf);
        this.interceptorClientId = (originalsClientId == null) ? "interceptor-producer-" + ClientIdGenerator.getInstance().nextPublisherId() : originalsClientId;
        LOG.info("LoggingProducerInterceptor is configured for client: {}", interceptorClientId);
    }

}
