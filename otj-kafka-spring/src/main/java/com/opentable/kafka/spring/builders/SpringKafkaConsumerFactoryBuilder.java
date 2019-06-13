package com.opentable.kafka.spring.builders;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SpringKafkaConsumerFactoryBuilder<K, V>  extends KafkaConsumerBaseBuilder<SpringKafkaConsumerFactoryBuilder<K, V>, K, V> {

    public static final String POLL_TIMEOUT_CONFIG = "poll-timeout";
    public static final String ACK_MODE_CONFIG = "ack-mode";
    public static final String CONCURRENCY_CONFIG = "concurrency";

    private Optional<Duration> pollTimeout = Optional.empty();
    private Optional<AckMode> ackMode = Optional.empty();
    private Optional<Integer> concurrency = Optional.empty();

    SpringKafkaConsumerFactoryBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider) {
        super(props, environmentProvider);
        readProperties();
    }

    private void readProperties() {
        readProperty(POLL_TIMEOUT_CONFIG, Duration::parse, this::withPollTimeout);
        readProperty(ACK_MODE_CONFIG, AckMode::valueOf, this::withAckMode);
        readProperty(CONCURRENCY_CONFIG, Integer::valueOf, this::withConcurrency);
    }

    @Override
    protected SpringKafkaConsumerFactoryBuilder<K, V> self() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K2, V2> SpringKafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        return (SpringKafkaConsumerFactoryBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K2, V2> SpringKafkaConsumerFactoryBuilder<K2, V2> withDeserializers(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        return (SpringKafkaConsumerFactoryBuilder<K2, V2>) super.withDeserializers(keyDeSer, valDeSer);
    }

    /**
     * Set the max time to block in the consumer waiting for records.
     * @param value the timeout.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withPollTimeout(Duration value) {
        this.pollTimeout = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set the ack mode to use when auto ack (in the configuration properties) is false.
     * <ul>
     * <li>RECORD: Ack after each record has been passed to the listener.</li>
     * <li>BATCH: Ack after each batch of records received from the consumer has been
     * passed to the listener</li>
     * <li>TIME: Ack after this number of milliseconds; (should be greater than
     * {@code #setPollTimeout(long) pollTimeout}.</li>
     * <li>COUNT: Ack after at least this number of records have been received</li>
     * <li>MANUAL: Listener is responsible for acking - use a
     * {@link org.springframework.kafka.listener.AcknowledgingMessageListener}.
     * </ul>
     * @param value the {@link AckMode}; default BATCH.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withAckMode(AckMode value) {
        withAutoCommit(false);
        this.ackMode = Optional.ofNullable(value);
        return self();
    }

    /**
     * Specify the container concurrency.
     * @param value the number of consumers to create.
     * @see ConcurrentMessageListenerContainer#setConcurrency(int)
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withConcurrency(Integer value) {
        this.concurrency = Optional.ofNullable(value);
        return self();
    }

    /**
     * Construct a factory with the provided configuration.
     */
    public ConsumerFactory<K, V> buildFactory() {
        return buildFactory(null, null);
    }

    /**
     * Construct a factory with the provided configuration and deserializers.
     * @param keyDeserializer the key {@link Deserializer}.
     * @param valueDeserializer the value {@link Deserializer}.
     */
    public <K2, V2> ConsumerFactory<K2, V2> buildFactory(@Nullable Deserializer<K2> keyDeserializer,
                                                         @Nullable Deserializer<V2> valueDeserializer) {
        return new DefaultKafkaConsumerFactory<>(buildProperties(), keyDeserializer, valueDeserializer);
    }

    /**
     * Constructs {@link KafkaListenerContainerFactory} with provided configuration.
     */
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> build() {
        final ConcurrentKafkaListenerContainerFactory<K, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(buildFactory());
        concurrency.ifPresent(factory::setConcurrency);
        final ContainerProperties containerProperties = factory.getContainerProperties();
        pollTimeout.map(Duration::toMillis).ifPresent(containerProperties::setPollTimeout);
        ackMode.ifPresent(containerProperties::setAckMode);
        return factory;
    }
}
