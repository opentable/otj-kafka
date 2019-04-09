package com.opentable.kafka.logging;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.opentable.conservedheaders.ConservedHeader;
import com.opentable.kafka.util.LogSamplerRandom;
import com.opentable.logging.CommonLogHolder;
import com.opentable.logging.otl.MsgV1;

public class LoggingUtils {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final byte[] FALSE = "false".getBytes(CHARSET);
    private static final byte[] TRUE = "true".getBytes(CHARSET);
    private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);

    @Nonnull
    public static <K, V> MsgV1 createEvent(ProducerRecord<K, V> record) {
        return MsgV1.builder()
            .logName("kafka-producer")
            .serviceType(CommonLogHolder.getServiceType())
            .uuid(UUID.randomUUID())
            .timestamp(Instant.now())
            .build();
    }

    @Nonnull
    public static <K, V> MsgV1 createEvent(ConsumerRecord<K, V> record) {
        return MsgV1.builder()
            .logName("kafka-producer")
            .serviceType(CommonLogHolder.getServiceType())
            .uuid(UUID.randomUUID())
            .timestamp(Instant.now())
            .build();
    }

    public static String getHeaderValue(final ConservedHeader header) {
        return MDC.get(header.getLogName());
    }

    public static String toString(Headers headers) {
        return Arrays.stream(headers.toArray())
            .map(h -> String.format("%s=%s", h.key(), new String(h.value())))
            .collect(Collectors.joining(", "));
    }

    public static <K, V> void setupHeaders(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        Arrays.asList(ConservedHeader.values()).forEach((header) -> {
            if (getHeaderValue(header) != null) {
                headers.add(header.getLogName(), getHeaderValue(header).getBytes(CHARSET));
            }
        });
        headers.add("ot-from-service", CommonLogHolder.getServiceType().getBytes(CHARSET));
    }

    public static <K, V> void setupTracing(LogSamplerRandom sampler, ProducerRecord<K, V> record) {
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

    public static <K, V> boolean isTraceNeeded(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        return StreamSupport.stream(headers.headers("ot-trace-message").spliterator(), false)
            .map(h -> new String(h.value()))
            .map("true"::equals)
            .filter(v -> v)
            .findFirst()
            .orElse(false);
    }

    public static <K, V> void trace(Logger log, String clientId, ProducerRecord<K, V> record) {
        if (isTraceNeeded(record)) {
            log.trace(createEvent(record).log(),
                "[Producer clientId={}] To:{}@{}, Headers:[{}], Message: {}",
                clientId, record.topic(), record.partition(), toString(record.headers()), record.value());
        }
    }

    public static <K, V> boolean isTraceNeeded(ConsumerRecord<K, V> record, LogSamplerRandom sampler) {
        final Headers headers = record.headers();
        return StreamSupport.stream(headers.headers("ot-trace-message").spliterator(), false)
            .map(h -> new String(h.value()))
            .map("true"::equals)
            .findFirst()
            .orElse(sampler.mark(record.topic()));
    }

    public static <K, V> void trace(Logger log, String clientId, String groupId, LogSamplerRandom sampler, ConsumerRecord<K, V> record) {
        if (isTraceNeeded(record, sampler)) {
            log.trace(createEvent(record).log(),
                "[Consumer clientId={}, groupId={}] From:{}@{}, Headers:[{}], Message: {}",
                clientId, groupId, record.topic(), record.partition(), toString(record.headers()), record.value());
        }
    }

    @Nonnull
    public static Map<ConservedHeader, String> extractHeaders(final Headers h) {
        final Map<ConservedHeader, String> headers = new EnumMap<>(ConservedHeader.class);
        for (final ConservedHeader header : ConservedHeader.values()) {
            final Iterator<Header> values = h.headers(header.getHeaderName()).iterator();
            if (values.hasNext()) {
                headers.put(header, new String(values.next().value()));
            }
            if (values.hasNext()) {
                LOG.warn("Request has '{}' header specified multiple times: {}", header,
                    values);
            }
        }
        // Check and conditionally sanitize request ID.
        String reqId = headers.get(ConservedHeader.REQUEST_ID);
        if (reqId != null) {
            try {
                UUID.fromString(reqId);
            } catch (final IllegalArgumentException e) {
                LOG.warn("Could not decode Tracking header '{}'", reqId, e);
                reqId = null;
            }
        }
        if (reqId == null) {
            headers.put(ConservedHeader.REQUEST_ID, UUID.randomUUID().toString());
        }
        return headers;
    }

    public static <K, V> void setupMDC(ConsumerRecord<K, V> record) {
        final Headers headers = record.headers();
        final Map<ConservedHeader, String> values = extractHeaders(headers);
        Arrays.asList(ConservedHeader.values()).forEach((header) -> {
            if (values.get(header) != null) {
                MDC.put(header.getLogName(), values.get(header));
            }
        });
    }
}
