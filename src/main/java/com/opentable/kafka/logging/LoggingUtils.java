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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;

import com.opentable.conservedheaders.ConservedHeader;
import com.opentable.logging.CommonLogFields;
import com.opentable.logging.CommonLogHolder;
import com.opentable.logging.otl.EdaMessageTraceV1;
import com.opentable.logging.otl.EdaMessageTraceV1.EdaMessageTraceV1Builder;
import com.opentable.logging.otl.MsgV1;
import com.opentable.service.AppInfo;

public class LoggingUtils {

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final byte[] FALSE = "false".getBytes(CHARSET);
    private static final byte[] TRUE = "true".getBytes(CHARSET);

    private static final String CLIENT_VERSION;

    private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);
    private static final String PROPERTIES_FILE_EXTENSION = ".properties";
    private static final String DEFAULT_VERSION = "unknown";
    private static final String ARTIFACT_ID = "otj-kafka";

    private final AppInfo appInfo;

    // I don't think this is needed at all. I believe adam's code does the trick.
    static {
        Resource resource = new ClassPathResource(ARTIFACT_ID + PROPERTIES_FILE_EXTENSION);
        String clientVersion = DEFAULT_VERSION;
        try {
            final Properties props = PropertiesLoaderUtils.loadProperties(resource);
            clientVersion = props.getProperty("version", DEFAULT_VERSION);
        } catch (IOException e) {
            LOG.warn("Cannot get client version for logging.", e);
        }
        CLIENT_VERSION = clientVersion;
    }

    public LoggingUtils(AppInfo appInfo) {
        this.appInfo = appInfo;
    }

    public Bucket getBucket(LoggingInterceptorConfig conf) {
        final Integer howOftenPer10Seconds = conf.getInt(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG);
        Bandwidth limit = null;
        if (howOftenPer10Seconds == null || howOftenPer10Seconds < 0) {
            LOG.warn("Not rate limiting");
            limit = Bandwidth.simple(Long.MAX_VALUE, Duration.ofMillis(1));
        } else {
            limit = Bandwidth.simple(howOftenPer10Seconds, Duration.ofSeconds(10));
        }
        return Bucket4j.builder().addLimit(limit).build();
    }

    private EdaMessageTraceV1Builder builder() {
        return EdaMessageTraceV1.builder()
                .edaClientName(ARTIFACT_ID)
                .edaClientVersion(CLIENT_VERSION)
                .edaClientPlatform("Java: " + System.getProperty("java.version"))
                .edaClientOs(System.getProperty("os.name"));
    }

    @Nonnull
    public <K, V> MsgV1 producerEvent(ProducerRecord<K, V> record, String clientId) {
        return builder()
            // msg-v1
            .logName("kafka-producer")
            .incoming(false)
            .serviceType(CommonLogHolder.getServiceType())
            .uuid(UUID.randomUUID())
            .timestamp(Instant.now())
            .requestId(optUuid(new String(record.headers().lastHeader(CommonLogFields.REQUEST_ID_KEY).value(), CHARSET)))
            .referringService(new String(record.headers().lastHeader(OTKafkaHeaders.REFERRING_SERVICE).value(), CHARSET))
            .referringHost(new String(record.headers().lastHeader(OTKafkaHeaders.REFERRING_HOST).value(), CHARSET))

            // eda-message-trace-v1
            .topic(record.topic())
            .partition(record.partition())
            .clientId(clientId)
            .recordKey(String.valueOf(record.key()))
            .recordValue(String.valueOf(record.value()))
            .recordTimestamp(record.timestamp())

            // from committed metadata
            //.recordKeySize(record.serializedKeySize())
            //.recordValueSize((record.serializedValueSize())
            //.offset(record.offset())
            .build();
    }

    @Nonnull
    public <K, V> MsgV1 consumerEvent(ConsumerRecord<K, V> record, String groupId, String clientId) {
        final Optional<Headers> headers = Optional.ofNullable(record.headers());
        return builder()
            // msg-v1
            .logName("kafka-consumer")
            .incoming(true)
            .serviceType(CommonLogHolder.getServiceType())
            .uuid(UUID.randomUUID())
            .timestamp(Instant.now())
            .requestId(optUuid(headers.map(h -> h.lastHeader((CommonLogFields.REQUEST_ID_KEY))).map(Header::value).map(String::new).orElse(null)))
            .referringService(headers.map(h -> h.lastHeader((OTKafkaHeaders.REFERRING_SERVICE))).map(Header::value).map(String::new).orElse(null))
            .referringHost(headers.map(h -> h.lastHeader((OTKafkaHeaders.REFERRING_HOST))).map(Header::value).map(String::new).orElse(null))

            // eda-message-trace-v1
            .topic(record.topic())
            .offset(record.offset())
            .partition(record.partition())
            .groupId(groupId)
            .clientId(clientId)
            .recordKeySize(record.serializedKeySize())
            .recordKey(String.valueOf(record.key()))
            .recordValueSize(record.serializedValueSize())
            .recordValue(String.valueOf(record.value()))
            .recordTimestamp(record.timestamp())
            .recordTimestampType(record.timestampType().name)
            .build();
    }

    public String getHeaderValue(final ConservedHeader header) {
        return MDC.get(header.getLogName());
    }

    public String toString(Headers headers) {
        return Arrays.stream(headers.toArray())
            .map(h -> String.format("%s=%s", h.key(), new String(h.value(), CHARSET)))
            .collect(Collectors.joining(", "));
    }

    public <K, V> void setupHeaders(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        Arrays.asList(ConservedHeader.values()).forEach((header) -> {
            if (getHeaderValue(header) != null) {
                headers.add(header.getLogName(), getHeaderValue(header).getBytes(CHARSET));
            }
        });
        setKafkaHeader(headers, OTKafkaHeaders.REFERRING_SERVICE, CommonLogHolder.getServiceType());
        setKafkaHeader(headers, OTKafkaHeaders.REFERRING_HOST, appInfo.getTaskHost());
        setKafkaHeader(headers, OTKafkaHeaders.REFERRING_INSTANCE_NO, appInfo.getInstanceNumber());
        setKafkaHeader(headers, OTKafkaHeaders.ENV, appInfo.getEnvInfo().getEnvironment());
        setKafkaHeader(headers, OTKafkaHeaders.ENV_FLAVOR, appInfo.getEnvInfo().getFlavor());
    }


    private void setKafkaHeader(Headers headers, String headerName, String value) {
        if (value != null) {
            headers.add(headerName, value.getBytes(CHARSET));
        }
    }

    private void setKafkaHeader(Headers headers, String headerName, Integer value) {
        if (value != null) {
            setKafkaHeader(headers, headerName, String.valueOf(value));
        }
    }

    public <K, V> void setupTracing(Bucket bucket, ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        if (!headers.headers(OTKafkaHeaders.TRACE_FLAG).iterator().hasNext()) {
            // If header not present, make decision our self and set it
            if (bucket.tryConsume(1)) {
                headers.add(OTKafkaHeaders.TRACE_FLAG, TRUE);
            } else {
                headers.add(OTKafkaHeaders.TRACE_FLAG, FALSE);
            }
        }
    }

    public <K, V> boolean isTraceNeeded(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        return StreamSupport.stream(headers.headers(OTKafkaHeaders.TRACE_FLAG).spliterator(), false)
            .map(h -> new String(h.value(), CHARSET))
            .map("true"::equals)
            .filter(v -> v)
            .findFirst()
            .orElse(false);
    }

    public <K, V> void trace(Logger log, String clientId, ProducerRecord<K, V> record) {
        if (isTraceNeeded(record)) {
            final MsgV1 event = producerEvent(record, clientId);
            MDC.put(CommonLogFields.REQUEST_ID_KEY, Objects.toString(event.getRequestId(), null));
            try {
                log.debug(event.log(),
                    "[Producer clientId={}] To:{}@{}, Headers:[{}], Message: {}",
                    clientId, record.topic(), record.partition(), toString(record.headers()), record.value());
            } finally {
                MDC.remove(CommonLogFields.REQUEST_ID_KEY);
            }
        }
    }

    public static <K, V> boolean isTraceNeeded(ConsumerRecord<K, V> record) {
        final Headers headers = record.headers();
        return StreamSupport.stream(headers.headers("ot-trace-message").spliterator(), false)
            .map(h -> new String(h.value(), CHARSET))
            .map("true"::equals)
            .findFirst()
            .orElse(false);
    }

    public <K, V> void trace(Logger log, String clientId, String groupId, Bucket bucket, ConsumerRecord<K, V> record) {
        if (isTraceNeeded(record) || bucket.tryConsume(1)) {
            final MsgV1 event = consumerEvent(record, groupId, clientId);
            MDC.put(CommonLogFields.REQUEST_ID_KEY, Objects.toString(event.getRequestId(), null));
            try {
                log.debug(event.log(),
                    "[Consumer clientId={}, groupId={}] From:{}@{}, Headers:[{}], Message: {}",
                    clientId, groupId, record.topic(), record.partition(), toString(record.headers()), record.value());
            } finally {
                MDC.remove(CommonLogFields.REQUEST_ID_KEY);
            }
        }
    }

    @Nonnull
    public Map<ConservedHeader, String> extractHeaders(final Headers h) {
        final Map<ConservedHeader, String> headers = new EnumMap<>(ConservedHeader.class);
        for (final ConservedHeader header : ConservedHeader.values()) {
            final Iterator<Header> values = h.headers(header.getHeaderName()).iterator();
            if (values.hasNext()) {

                headers.put(header, new String(values.next().value(), CHARSET));
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


    private static UUID optUuid(String uuid) {
        try {
            return uuid == null ? null : UUID.fromString(uuid);
        } catch (IllegalArgumentException e) {
            LOG.warn("Unable to parse purported request id '{}': {}", uuid, e.toString());
            return null;
        }
    }

}
