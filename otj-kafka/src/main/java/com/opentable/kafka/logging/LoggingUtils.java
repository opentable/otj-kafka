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
import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.logging.CommonLogHolder;
import com.opentable.logging.otl.EdaMessageTraceV1;
import com.opentable.logging.otl.EdaMessageTraceV1.EdaMessageTraceV1Builder;
import com.opentable.logging.otl.MsgV1;

/**
 * General logging code and logic. Builds various OTL records, headers for metadata, etc.
 */
class LoggingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingUtils.class);

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final String UNKNOWN = "unknown";
    private static final byte[] FALSE = "false".getBytes(CHARSET);
    private static final byte[] TRUE = "true".getBytes(CHARSET);

    private static final String PROPERTIES_FILE_EXTENSION = ".properties";
    private static final String DEFAULT_VERSION = "unknown";
    private static final String ARTIFACT_ID = "otj-kafka";

    private final EnvironmentProvider environmentProvider;
    private final String libraryVersion;
    private final String kafkaVersion;
    private final String javaVersion;
    private final String os;
    private final Bucket errLogging;
    // See comments under addHeader - TODO: integrate with Opentracing context
    private final String traceId = UUID.randomUUID().toString();

    LoggingUtils(EnvironmentProvider environmentProvider) {
        this.environmentProvider = environmentProvider;
        this.javaVersion = System.getProperty("java.runtime.version", UNKNOWN);
        this.os = System.getProperty("os.name", UNKNOWN);
        this.errLogging = getBucket(Bandwidth.simple(10, Duration.ofMinutes(1)));

        this.libraryVersion = getVersion(ARTIFACT_ID + PROPERTIES_FILE_EXTENSION, "kafka.logging.version", DEFAULT_VERSION);
        this.kafkaVersion = getVersion("/kafka/kafka-version.properties", "kafka.version.version", DEFAULT_VERSION);
    }

    /**
     * Read a classpath resource, looking for the version property.
     * @param classPathResourceName path on classpath
     * @param systemPropertyName fall back if version is missing to reading this system property. null indicates skip this
     * @param defaultVersion finally if all else fails return this value.
     * @return derived value
     */
    private String getVersion(String classPathResourceName, String systemPropertyName, String defaultVersion) {
        String clientVersion = defaultVersion;
        try {
            final Resource resource = new ClassPathResource(classPathResourceName);
            final Properties props = PropertiesLoaderUtils.loadProperties(resource);
            clientVersion = props.getProperty("version",
                    systemPropertyName == null ? defaultVersion :
                            System.getProperty(systemPropertyName, defaultVersion));
        } catch (IOException e) {
            if (errLogging.tryConsume(1)) {
                LOG.warn("Cannot get client version for logging.", e);
            }
        }
        return clientVersion;
    }


    private Bucket getBucket(Bandwidth bandWidth) {
        return Bucket4j.builder().addLimit(bandWidth).build();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private EdaMessageTraceV1Builder builder(final Optional<Headers> headers) {
        return EdaMessageTraceV1.builder()
                .kafkaVersion(kafkaVersion)
                .kafkaClientName(ARTIFACT_ID)
                .kafkaClientVersion(libraryVersion)
                .kafkaClientPlatform("java")
                .kafkaClientPlatformVersion(javaVersion)
                .kafkaClientOs(os)
                .uuid(UUID.randomUUID())
                .timestamp(Instant.now())
                .serviceType(CommonLogHolder.getServiceType())
                .requestId(ensureUUID(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.REQUEST_ID)))).map(Header::value).map(String::new).orElse(null)))
                .referringService(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.REFERRING_SERVICE)))).map(Header::value).map(String::new).orElse(null))
                .referringHost(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.REFERRING_HOST)))).map(Header::value).map(String::new).orElse(null))
                .otEnv(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.ENV)))).map(Header::value).map(String::new).orElse(null))
                .otEnvFlavor(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.ENV_FLAVOR)))).map(Header::value).map(String::new).orElse(null))
                .instanceNo(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.REFERRING_INSTANCE_NO))))
                        .map(Header::value).map(String::new).map(this::parse).orElse(null))
                .otParentSpanId(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.PARENT_SPAN_ID))))
                        .map(Header::value).map(String::new).orElse(null))
                .otSpanId(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.SPAN_ID))))
                        .map(Header::value).map(String::new).orElse(UUID.randomUUID().toString())) // not nullable
                .otTraceId(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.TRACE_ID))))
                        .map(Header::value).map(String::new).orElse(UUID.randomUUID().toString())) // not nullable
                // See https://github.com/apache/incubator-zipkin-b3-propagation - set to 1 if and only if trace is on
                .otFlags(headers.map(h -> h.lastHeader((kn(OTKafkaHeaders.TRACE_FLAG))))
                        .map(Header::value).map(String::new).filter("true"::equals).map(t -> "1").orElse(null))

                ;
    }

    private Integer parse(final String t) {
        try {
            return Integer.parseInt(t);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private String kn(final OTKafkaHeaders otKafkaHeaders) {
        return otKafkaHeaders.getKafkaName();
    }

    @Nonnull
    private <K, V> MsgV1 producerEvent(ProducerRecord<K, V> record, String clientId) {
        final Optional<Headers> headers = Optional.ofNullable(record.headers());
        final EdaMessageTraceV1 edaMessageTraceV1 = builder(headers)
                // msg-v1
                .logName("kafka-producer")
                .incoming(false)

                // eda-message-trace-v1
                .kafkaTopic(record.topic())
                .kafkaPartition(record.partition())
                .kafkaClientId(clientId)
                .kafkaRecordKey(String.valueOf(record.key()))
                .kafkaRecordValue(String.valueOf(record.value()))
                .kafkaRecordTimestamp(record.timestamp())

                // from committed metadata
                //.recordKeySize(record.serializedKeySize())
                //.recordValueSize((record.serializedValueSize())
                //.offset(record.offset())
                .build();
        debugEvent(edaMessageTraceV1);
        return edaMessageTraceV1;
    }

    @Nonnull
    private <K, V> MsgV1 consumerEvent(ConsumerRecord<K, V> record, String groupId, String clientId) {
        final Optional<Headers> headers = Optional.ofNullable(record.headers());
        final EdaMessageTraceV1 edaMessageTraceV1 = builder(headers)
            // msg-v1
            .logName("kafka-consumer")
            .incoming(true)

            // eda-message-trace-v1
            .kafkaTopic(record.topic())
            .kafkaOffset(record.offset())
            .kafkaPartition(record.partition())
            .kafkaGroupId(groupId)
            .kafkaClientId(clientId)
            .kafkaRecordKeySize(record.serializedKeySize())
       //     .kafkaRecordKey(String.valueOf(record.key()))
            .kafkaRecordValueSize(record.serializedValueSize())
         //   .kafkaRecordValue(String.valueOf(record.value()))
            .kafkaRecordTimestamp(record.timestamp())
            .kafkaRecordTimestampType(record.timestampType() == null ? TimestampType.NO_TIMESTAMP_TYPE.name : record.timestampType().name)
            .build();
        debugEvent(edaMessageTraceV1);
        return edaMessageTraceV1;
    }

    protected void debugEvent(final EdaMessageTraceV1 edaMessageTraceV1) {
        /* do nothing */
    }

    /**
     * Grab value from MDC
     * @param header header type
     * @return value
     */
    private String getHeaderValue(final ConservedHeader header) {
        return MDC.get(header.getLogName());
    }

    /**
     * Output all headers as
     *  - headerName=Value (comma delimited)
     * @param headers headers collection
     * @return String
     */
    private String formatHeaders(Headers headers) {
        return Arrays.stream(headers.toArray())
            // Don't include items that we know go in the otl
            .filter(t -> !OTKafkaHeaders.DEFINED_HEADERS.contains(t.key()))
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
    <K, V> void addHeaders(ProducerRecord<K, V> record) {
          final Headers headers = record.headers();
        Arrays.asList(ConservedHeader.values()).forEach((header) -> {
            if (getHeaderValue(header) != null) {
                headers.add(header.getLogName(), getHeaderValue(header).getBytes(CHARSET));
            }
        });
        /*
         * Note: Currently the traceId is generated at instantiation and parentSpanId is always null.
         * This is because UNTIL we connect to OpenTracing's Context, we cannot create and propagate correctly.
         * It is INCORRECT to propagate via the MDC - Opentracing uses its own internal context (threadlocal or reactive)
         */
        final String traceId = getCurrentTraceId();
        // Just this span
        final String currentSpanId = UUID.randomUUID().toString();
        // Parent
        final String parentSpanId = getParentSpanId();
        setKafkaHeader(headers, OTKafkaHeaders.TRACE_ID, traceId);
        setKafkaHeader(headers, OTKafkaHeaders.SPAN_ID, currentSpanId);
        setKafkaHeader(headers, OTKafkaHeaders.PARENT_SPAN_ID, parentSpanId);
        setKafkaHeader(headers, OTKafkaHeaders.REFERRING_SERVICE, environmentProvider.getReferringService());
        setKafkaHeader(headers, OTKafkaHeaders.REFERRING_HOST, environmentProvider.getReferringHost());
        setKafkaHeader(headers, OTKafkaHeaders.REFERRING_INSTANCE_NO, environmentProvider.getReferringInstanceNumber());
        setKafkaHeader(headers, OTKafkaHeaders.ENV, environmentProvider.getEnvironment());
        setKafkaHeader(headers, OTKafkaHeaders.ENV_FLAVOR, environmentProvider.getEnvironmentFlavor());
    }

    private String getCurrentTraceId() {
        return this.traceId;
    }

    private String getParentSpanId() {
        return null;
    }

    /**
     * Set the header only if the value isn't null
     * @param headers kafka headers
     * @param headerName name
     * @param value value
     */
    private void setKafkaHeader(Headers headers, OTKafkaHeaders headerName, String value) {
        if (value != null && headers != null && headerName != null) {
            headers.add(headerName.getKafkaName(), value.getBytes(CHARSET));
        }
    }

    /**
     * Set the header only if the value isn't null
     * @param headers kafka headers
     * @param headerName name
     * @param value value
     */
    private void setKafkaHeader(Headers headers, OTKafkaHeaders headerName, Integer value) {
        if (value != null && (headerName != null)) {
            setKafkaHeader(headers, headerName, String.valueOf(value));
        }
    }

    /**
     * This checks if the tracing flag is on. Perhaps someone set it manually?
     * Otherwise it is set if the log limit has not been reached.
     * We'll use this tracing flag to determine logging
     * @param sampler Log sampler
     * @param record record. Headers will be mutated.
     */
    <K, V> boolean setTracingHeader(LogSampler sampler, ProducerRecord<Object, Object> record, GenerateHeaders propagateHeaders) {
        if (propagateHeaders == GenerateHeaders.NONE) {
            // We aren't going to set the header, so whether we should log is just dependent on the rate limit
            return sampler.mark(record.topic());
        }
        // GenerateHeaders therefore must be ALL or TRACE, so ok to add a trace header.
        // Now, let's check if there's an existing trace flag
        final Headers headers  = record.headers();
        final String traceFlag = kn(OTKafkaHeaders.TRACE_FLAG);
        final Header lastTraceHeaderIfAny = headers.lastHeader(traceFlag);
        final boolean previouslySetByUser = lastTraceHeaderIfAny != null &&
                Boolean.parseBoolean(new String(lastTraceHeaderIfAny.value(), StandardCharsets.UTF_8));

        // Trace flag present and equal to true
        if (previouslySetByUser) {
            // There's a trace flag set already, let's log
            return true;
        }

        // Trace flag present and equal to false
        if (lastTraceHeaderIfAny != null) {
            return false;
        }

        // No trace flag currently present, depends on rate limit
        if (sampler.mark(record.topic())) {
            headers.add(traceFlag, TRUE);
            return true;
        } else {
            headers.add(traceFlag, FALSE);
            return false;
        }
    }


    private <K, V> boolean isTraceFlagEnabled(Headers headers) {
        final String traceFlag = kn(OTKafkaHeaders.TRACE_FLAG);
        return StreamSupport.stream(headers.headers(traceFlag).spliterator(), false)
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
    <K, V> void logProducer(Logger log, String clientId, ProducerRecord<K, V> record) {
            final MsgV1 event = producerEvent(record, clientId);
            log.debug(event.log(),
                    "Producer clientId={}] Headers: {}",
                    clientId, formatHeaders(record.headers()));

    }

    /**
     * Check if the trace flag is set. This convenience method compensates
     * for the fact that Kafka doesn't have any base class for xxxRecords.
     * @param record consumer record
     * @param <K> key
     * @param <V> value
     * @return true, if trace flag set
     */
    private <K, V> boolean isTraceFlagEnabled(ConsumerRecord<K, V> record) {
        return isTraceFlagEnabled(record.headers());
    }

    /**
     * Log a consumer record if either a trace flag is passed, or we have tokens remaining
     * @param record record
     * @param sampler log sampler
     * @param <K> key
     * @param <V> value
     * @return true, if logging is needed
     */
    private <K, V> boolean isLoggingNeeded(ConsumerRecord<K, V> record, LogSampler sampler) {
        return isTraceFlagEnabled(record) || sampler.mark(record.topic());
    }

    /**
     * Logging logic for the consumer
     * @param log logger
     * @param clientId clientId
     * @param groupId groupId
     * @param sampler log sampler
     * @param record consumer record
     * @param <K> key
     * @param <V> value
     */
    <K, V> void maybeLogConsumer(Logger log, String clientId, String groupId, LogSampler sampler, ConsumerRecord<K, V> record) {
        if (isLoggingNeeded(record, sampler)) {
            final MsgV1 event = consumerEvent(record, groupId, clientId);
            log.debug(event.log(),
                    "Consumer clientId={}, groupId={} Headers: {}",
                    clientId, groupId, formatHeaders(record.headers()));
        }
    }

    /**
     * Take a string, possibly a null one, and check if it parses to a UUID
     * If it does, return it, otherwise log and return a random one
     * @param uuids string
     * @return UUID
     */
    private UUID ensureUUID(String uuids) {
        try {
            if (uuids != null) {
                return UUID.fromString(uuids);
            } else {
                return null;
            }
        } catch (IllegalArgumentException e) {
            if (errLogging.tryConsume(1)) {
                LOG.trace("Unable to parse purported request id '{}': {}", uuids, e);
            }
        }
        return UUID.randomUUID();
    }

}
