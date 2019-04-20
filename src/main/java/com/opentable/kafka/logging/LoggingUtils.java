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
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
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
    private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);
    private static final String PROPERTIES_FILE_EXTENSION = ".properties";
    private static final String DEFAULT_VERSION = "unknown";
    private static final String ARTIFACT_ID = "otj-kafka";

    private final AppInfo appInfo;
    private final String libraryVersion;
    private final String kafkaVersion;

    public LoggingUtils(AppInfo appInfo) {
        this.appInfo = appInfo;
        this.libraryVersion = getVersion(ARTIFACT_ID + PROPERTIES_FILE_EXTENSION, "kafka.logging.version", DEFAULT_VERSION);
        this.kafkaVersion = getVersion("/kafka/kafka-version.properties", "kafka.version.version", DEFAULT_VERSION);
    }

    private String getVersion(String classPathResourceName, String systemPropertyName, String defaultVersion) {
        String clientVersion = defaultVersion;
        try {
            final Resource resource = new ClassPathResource(classPathResourceName);
            final Properties props = PropertiesLoaderUtils.loadProperties(resource);
            clientVersion = props.getProperty("version", System.getProperty(systemPropertyName, DEFAULT_VERSION));
        } catch (IOException e) {
            LOG.warn("Cannot get client version for logging.", e);
        }
        return clientVersion;
    }

    /**
     * Generate a fresh new token bucket
     * @param conf the configuration, used to determine the token refresh rate
     * @return a bucket
     */
    Bucket getBucket(LoggingInterceptorConfig conf) {
        final Integer howOftenPer10Seconds = conf.getInt(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG);
        Bandwidth limit;
        if (howOftenPer10Seconds == null || howOftenPer10Seconds < 0) {
            LOG.warn("Not rate limiting");
            // Apparently the only way to be "unlimited"
            limit = Bandwidth.simple(Long.MAX_VALUE, Duration.ofSeconds(1));
        } else {
            limit = Bandwidth.simple(howOftenPer10Seconds, Duration.ofSeconds(10));
        }
        return Bucket4j.builder().addLimit(limit).build();
    }

    private EdaMessageTraceV1Builder builder() {
        return EdaMessageTraceV1.builder()
                .edaClientName(ARTIFACT_ID)
                .edaClientVersion(libraryVersion)
                .edaClientPlatform("Java: " + System.getProperty("java.version"))
                .edaClientOs(System.getProperty("os.name"))
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now())
                .serviceType(CommonLogHolder.getServiceType());
    }

    @Nonnull
    public <K, V> MsgV1 producerEvent(ProducerRecord<K, V> record, String clientId) {
        return builder()
            // msg-v1
            .logName("kafka-producer")
            .incoming(false)
            .requestId(checkIfGoodUUID(new String(unk(record.headers().lastHeader(CommonLogFields.REQUEST_ID_KEY)), CHARSET)))
            .referringService(new String(unk(record.headers().lastHeader(OTKafkaHeaders.REFERRING_SERVICE)), CHARSET))
            .referringHost(new String(unk(record.headers().lastHeader(OTKafkaHeaders.REFERRING_HOST)), CHARSET))

            // eda-message-trace-v1
            .topic(record.topic())
            .partition(record.partition())
            .clientId(clientId)
                // again these two fail for binary data
            .recordKey(String.valueOf(record.key()))
            .recordValue(String.valueOf(record.value()))
            .recordTimestamp(record.timestamp())

            // from committed metadata
            //.recordKeySize(record.serializedKeySize())
            //.recordValueSize((record.serializedValueSize())
            //.offset(record.offset())
            .build();
    }

    private static final byte[] UNKNOWN = "unknown".getBytes(CHARSET);

    /**
     * Convenience method to guard against a missing value
     * @param lastHeader header
     * @return the value, or UNKNOWN if missing
     */
    private byte[] unk(final Header lastHeader) {
        return lastHeader == null ? UNKNOWN : lastHeader.value();
    }

    @Nonnull
    private <K, V> MsgV1 consumerEvent(ConsumerRecord<K, V> record, String groupId, String clientId) {
        final Optional<Headers> headers = Optional.ofNullable(record.headers());
        return builder()
            // msg-v1
            .logName("kafka-consumer")
            .incoming(true)
            .requestId(checkIfGoodUUID(headers.map(h -> h.lastHeader((CommonLogFields.REQUEST_ID_KEY))).map(Header::value).map(String::new).orElse(null)))
            .referringService(headers.map(h -> h.lastHeader((OTKafkaHeaders.REFERRING_SERVICE))).map(Header::value).map(String::new).orElse(null))
            .referringHost(headers.map(h -> h.lastHeader((OTKafkaHeaders.REFERRING_HOST))).map(Header::value).map(String::new).orElse(null))

            // eda-message-trace-v1
            .topic(record.topic())
            .offset(record.offset())
            .partition(record.partition())
            .groupId(groupId)
            .clientId(clientId)
            .recordKeySize(record.serializedKeySize())
                // this will fail for binary data
            .recordKey(String.valueOf(record.key()))
            .recordValueSize(record.serializedValueSize())
                // This will fail for binary data
            .recordValue(String.valueOf(record.value()))
            .recordTimestamp(record.timestamp())
            .recordTimestampType(record.timestampType() == null ? TimestampType.NO_TIMESTAMP_TYPE.name : record.timestampType().name)
            .build();
    }

    private String getHeaderValue(final ConservedHeader header) {
        return MDC.get(header.getLogName());
    }

    private String toString(Headers headers) {
        return Arrays.stream(headers.toArray())
            .map(h -> String.format("%s=%s", h.key(), new String(h.value(), CHARSET)))
            .collect(Collectors.joining(", "));
    }

    /**
     * Sets up headers.
     * If "conserved headers" are available in the MDC, we copy them over to the Header
     * In addition, we add various other diagnostic headers, mostly from AppInfo
     * @param record record
     * @param <K> key
     * @param <V> value
     */
    <K, V> void setupHeaders(ProducerRecord<K, V> record) {
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

    /**
     * This checks if the tracing flag is on. Perhaps someone set it manually?
     * Otherwise it is set if the log limit has not been reached.
     * We'll use this tracing flag to determine logging
     * @param bucket token bucket
     * @param headers headers of record. These will be mutated.
     */
    <K, V> void setupTracing(Bucket bucket,  Headers headers) {
        if (!headers.headers(OTKafkaHeaders.TRACE_FLAG).iterator().hasNext()) {
            // If header not present, make decision our self and set it
            if (bucket.tryConsume(1)) {
                headers.add(OTKafkaHeaders.TRACE_FLAG, TRUE);
            } else {
                headers.add(OTKafkaHeaders.TRACE_FLAG, FALSE);
            }
        }
    }

    /**
     * Determining whether the ProducerRecord should be logged at this point is simple - We look for the trace flag,
     * and if it exists, we log
     * @param record record
     * @param <K> key
     * @param <V> value
     * @return true, if logging is needed.
     */
    private <K, V> boolean isLoggingNeeded(ProducerRecord<K, V> record) {
        final Headers headers = record.headers();
        return StreamSupport.stream(headers.headers(OTKafkaHeaders.TRACE_FLAG).spliterator(), false)
            .map(h -> new String(h.value(), CHARSET))
            .map("true"::equals)
            .filter(v -> v)
            .findFirst()
            .orElse(false);
    }

    /**
     * For the producer interceptor, this determines if we are going to log, and if so, builds the otl log record and outputs
     * @param log logger, for setting level
     * @param clientId The clientId
     * @param record the Record
     * @param <K> key
     * @param <V> value
     */
    <K, V> void trace(Logger log, String clientId, ProducerRecord<K, V> record) {
        if (isLoggingNeeded(record)) {
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

    /**
     * Check if the trace flag is set
     * @param record consumer recod
     * @param <K> key
     * @param <V> value
     * @return true, if trace flag set
     */
    private <K, V> boolean isTraceFlagEnabled(ConsumerRecord<K, V> record) {
        final Headers headers = record.headers();
        return StreamSupport.stream(headers.headers("ot-trace-message").spliterator(), false)
            .map(h -> new String(h.value(), CHARSET))
            .map("true"::equals)
            .findFirst()
            .orElse(false);
    }

    /**
     * Log a consumer record if either a trace flag is passed, or we have tokens remaining
     * @param record record
     * @param bucket token bucket
     * @param <K> key
     * @param <V> value
     * @return true, if logging is needed
     */
    private <K, V> boolean isLoggingNeeded(ConsumerRecord<K, V> record, Bucket bucket) {
        return isTraceFlagEnabled(record) || bucket.tryConsume(1);
    }

    /**
     * Logging logic for the consumer
     * @param log logger
     * @param clientId clientId
     * @param groupId groupId
     * @param bucket token bucket
     * @param record consumer record
     * @param <K> key
     * @param <V> value
     */
    <K, V> void trace(Logger log, String clientId, String groupId, Bucket bucket, ConsumerRecord<K, V> record) {
        if (isLoggingNeeded(record, bucket)) {
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

    /**
     * Take a string, possibly a null one, and check if it parses to a UUID
     * If it does, return it, otherwise log and return a random one
     * @param uuids string
     * @return UUID
     */
    private UUID checkIfGoodUUID(String uuids) {
        try {
            if (uuids != null) {
                return UUID.fromString(uuids);
            } else {
                throw new IllegalArgumentException("UUID is null");
            }
        } catch (IllegalArgumentException e) {
            LOG.warn("Unable to parse purported request id '{}': {}", uuids, e);
        }
        return UUID.randomUUID();
    }

    /**
     * Provides a simple way to make sure all clientIds are unique.
     */
    static class ClientIdGenerator {
        static final ClientIdGenerator INSTANCE = new ClientIdGenerator();
        private final AtomicInteger consumerIds = new AtomicInteger(0);
        private final AtomicInteger producerIds = new AtomicInteger(0);
        int nextConsumerId() {
            return consumerIds.getAndIncrement();
        }

        int nextPublisherId() {
            return producerIds.getAndIncrement();
        }
    }
}
