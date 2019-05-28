package com.opentable.kafka.builders;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class KafkaConsumerBaseBuilder<SELF extends KafkaConsumerBaseBuilder<SELF, K, V>, K, V> extends KafkaBaseBuilder<SELF> {

    private Optional<String> groupId = Optional.empty();
    private Optional<Boolean> enableAutoCommit = Optional.empty();
    private OptionalInt maxPollRecords = OptionalInt.empty();
    private OptionalInt maxPartitionFetch = OptionalInt.empty();
    private OptionalLong sessionTimeoutMs = OptionalLong.empty();
    private OptionalLong maxPollIntervalMs = OptionalLong.empty();
    private Optional<KafkaConsumerBuilder.AutoOffsetResetType> autoOffsetResetType = Optional.empty();
    private Class<? extends PartitionAssignor> partitionStrategy = RangeAssignor.class;
    // Kafka is really stupid. In the properties you can only configure a no-args
    // and then they hack around it if you have one supplied
    private Class<? extends Deserializer<K>> keyDe;
    private Class<? extends Deserializer<V>> valueDe;
    protected Deserializer<K> keyDeserializerInstance;
    protected Deserializer<V> valueDeserializerInstance;

    protected KafkaConsumerBaseBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
    }

    public SELF withGroupId(String val) {
        groupId = Optional.ofNullable(val);
        return self();
    }

    public SELF withAutoOffsetReset(KafkaConsumerBuilder.AutoOffsetResetType val) {
        autoOffsetResetType = Optional.ofNullable(val);
        return self();
    }

    public SELF withMaxPollRecords(int val) {
        maxPollRecords = OptionalInt.of(val);
        return self();
    }

    /**
     * Provide an class. If you have a no-args constructor use this
     * @param keyDeSer key deserializer
     * @param valDeSer value deserializer
     * @param <K2> The type of the key returned by de-serializer
     * @param <V2> The type of the value returned by de-serializer
     * @return this
     */
    protected <K2, V2> KafkaConsumerBaseBuilder<?, K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        KafkaConsumerBaseBuilder<? extends SELF, K2, V2> res = (KafkaConsumerBaseBuilder<? extends SELF, K2, V2>) self();
        res.keyDe = keyDeSer;
        res.valueDe = valDeSer;
        res.keyDeserializerInstance = null;
        res.valueDeserializerInstance = null;
        return res;
    }

    /**
     * Provide an instance. If you don't have a no-args constructor use this
     * @param keyDeSer key deserializer
     * @param valDeSer value deserializer
     * @param <K2> The type of the key returned by de-serializer
     * @param <V2> The type of the value returned by de-serializer
     * @return this
     */
    protected <K2, V2> KafkaConsumerBaseBuilder<?, K2, V2> withDeserializers(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        KafkaConsumerBaseBuilder<? extends SELF, K2, V2> res = (KafkaConsumerBaseBuilder<? extends SELF, K2, V2>) self();
        res.keyDeserializerInstance = keyDeSer;
        res.valueDeserializerInstance = valDeSer;
        this.keyDe = null;
        this.valueDe = null;
        return res;
    }

    public SELF withPartitionAssignmentStrategy(Class<? extends PartitionAssignor> partitionAssignmentStrategy) {
        partitionStrategy = partitionAssignmentStrategy;
        return self();
    }


    public SELF withAutoCommit(boolean val) {
        enableAutoCommit = Optional.of(val);
        return self();
    }


    public SELF withMaxPartitionFetchBytes(int bytes) {
        this.maxPartitionFetch = OptionalInt.of(bytes);
        return self();
    }


    public SELF withPollInterval(Duration duration) {
        if (duration != null) {
            maxPollIntervalMs = OptionalLong.of(duration.toMillis());
        }
        return self();
    }

    public SELF withSessionTimeoutMs(Duration duration) {
        if (duration != null) {
            sessionTimeoutMs = OptionalLong.of(duration.toMillis());
        }
        return self();
    }

    public SELF withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        this.addInterceptor(clazz.getName());
        return self();
    }

    public Map<String, Object> buildProperties() {
        internalBuild();
        return getFinalProperties();
    }

    protected void internalBuild() {
        if (partitionStrategy != null) {
            addProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionStrategy.getName());
        }
        maxPartitionFetch.ifPresent(m -> this.addProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(m)));
        maxPollIntervalMs.ifPresent(m -> this.addProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(m)));
        sessionTimeoutMs.ifPresent(s -> this.addProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(s)));
        enableAutoCommit.ifPresent(e -> this.addProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(e)));
        this.setupInterceptors(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        groupId.ifPresent(gid -> this.addProperty(ConsumerConfig.GROUP_ID_CONFIG, gid));
        autoOffsetResetType.ifPresent(a -> this.addProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, a.value));
        maxPollRecords.ifPresent(mpr -> this.addProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, mpr));
        if (keyDe != null) {
            this.addProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDe.getName());
        }
        if (valueDe != null) {
            this.addProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDe.getName());
        }
        // Merge in common and user supplied properties.
        this.finishBuild();
        this.cantBeNull(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "Partition assignment strategy can't be null");
    }

    public enum AutoOffsetResetType {
        Latest("latest"), Earliest("earliest"), None("none");
        final String value;
        AutoOffsetResetType(String value) {
            this.value = value;
        }

        public static AutoOffsetResetType fromString(String c) {
            return Arrays.stream(values()).filter(t -> t.value.equalsIgnoreCase(c))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Can't convert " + c));
        }
    }


}
