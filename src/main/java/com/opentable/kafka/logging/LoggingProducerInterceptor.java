package com.opentable.kafka.logging;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import com.opentable.logging.CommonLogHolder;
import com.opentable.logging.otl.MsgV1;

public class LoggingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingProducerInterceptor.class);
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private String originalsClientId;
    private String interceptorClientId;

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        Arrays.asList(ConservedHeader.values()).forEach((header) -> {
            if (this.getHeaderValue(header) != null) {
                record.headers().add(header.getLogName(), this.getHeaderValue(header).getBytes(CHARSET));
            }
        });
        record.headers().add("ot-from-service", CommonLogHolder.getServiceType().getBytes(CHARSET));
        LOG.info(createEvent(record).log(),
            "[Producer clientId={}] To:{}@{}, Headers:[{}], Message: {}",
            interceptorClientId, record.topic(), record.partition(), toString(record.headers()), record.value());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {
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

    private static class ClientIdGenerator {
        private static final AtomicInteger IDS = new AtomicInteger(0);
        static int nextClientId() {
            return IDS.getAndIncrement();
        }
    }

}
