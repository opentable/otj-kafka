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
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.opentable.conservedheaders.ConservedHeader;
import com.opentable.kafka.util.LogSamplerRandom;
import com.opentable.logging.CommonLogHolder;
import com.opentable.logging.otl.MsgV1;

public class LoggingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);
    private static final Charset CHARSET = Charset.forName("UTF-8");
    public static final byte[] FALSE = "false".getBytes(CHARSET);
    public static final byte[] TRUE = "true".getBytes(CHARSET);

    private String originalsClientId;
    private String interceptorClientId;
    private LogSamplerRandom sampler = new LogSamplerRandom(5.0);

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        setupHeaders(record);
        setupTracing(record);
        //trace(record);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {
        LOG.info("metadata: {}", metadata);
        if (e != null) {
            LOG.error("", e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        this.originalsClientId = (String) config.get(ProducerConfig.CLIENT_ID_CONFIG);
        this.interceptorClientId = (originalsClientId == null) ? "interceptor-producer-" + ClientIdGenerator.nextClientId() : originalsClientId;
        LOG.info("LoggingProducerInterceptor is configured for client: {}", interceptorClientId);
    }

    @Nonnull
    protected MsgV1 createEvent(ProducerRecord<K, V> record) {
        return MsgV1.builder()
            .logName("kafka-producer")
            .serviceType(CommonLogHolder.getServiceType())
            .uuid(UUID.randomUUID())
            .timestamp(Instant.now())
            .build();
    }

    private String getHeaderValue(final ConservedHeader header) {
        return MDC.get(header.getLogName());
    }

    private String toString(Headers headers) {
        return Arrays.stream(headers.toArray())
        .map(h -> String.format("%s=%s", h.key(), new String(h.value())))
        .collect(Collectors.joining(", "));
    }

    private void setupHeaders(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        Arrays.asList(ConservedHeader.values()).forEach((header) -> {
            if (this.getHeaderValue(header) != null) {
                headers.add(header.getLogName(), this.getHeaderValue(header).getBytes(CHARSET));
            }
        });
        headers.add("ot-from-service", CommonLogHolder.getServiceType().getBytes(CHARSET));
    }


    private void setupTracing(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        if (!headers.headers("ot-trace-message").iterator().hasNext()) {
            // If header not present, make decision our self and set it
            if (sampler.mark(record.topic())) {
                headers.add("ot-trace-message", TRUE);
            } else {
                headers.add("ot-trace-message", FALSE);
            }
        }
    }

    private void trace(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        final Boolean trace = StreamSupport.stream(headers.headers("ot-trace-message").spliterator(), false)
            .map(h -> new String(h.value()))
            .map("true"::equals)
            .filter(v -> v)
            .findFirst()
            .orElse(false);
        if (trace) {
            LOG.info(createEvent(record).log(),
                "[Producer clientId={}] To:{}@{}, Headers:[{}], Message: {}",
                interceptorClientId, record.topic(), record.partition(), toString(record.headers()), record.value());
        }
    }

    private static class ClientIdGenerator {
        private static final AtomicInteger IDS = new AtomicInteger(0);
        static int nextClientId() {
            return IDS.getAndIncrement();
        }
    }

}
