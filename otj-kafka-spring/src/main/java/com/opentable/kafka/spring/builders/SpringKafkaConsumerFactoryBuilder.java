package com.opentable.kafka.spring.builders;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.support.converter.BytesJsonMessageConverter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.lang.Nullable;

import com.opentable.kafka.builders.EnvironmentProvider;
import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SpringKafkaConsumerFactoryBuilder<K, V>  extends KafkaConsumerBaseBuilder<SpringKafkaConsumerFactoryBuilder<K, V>, K, V> {

    public static final String POLL_TIMEOUT_CONFIG = "poll-timeout";
    public static final String ACK_MODE_CONFIG = "ack-mode";
    public static final String CONCURRENCY_CONFIG = "concurrency";
    public static final String SYNC_COMMITS_CONFIG = "sync-commits";
    public static final String ACK_ON_ERROR_CONFIG = "ack-on-error";

    private final Optional<ObjectMapper> objectMapper;

    private Optional<Duration> pollTimeout = Optional.empty();
    private Optional<AckMode> ackMode = Optional.empty();
    private Optional<Integer> concurrency = Optional.empty();
    private Optional<Consumer<ContainerProperties>> containerPropertiesCustomizer = Optional.empty();
    private Optional<Consumer<KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>>>> factoryCustomizer = Optional.empty();
    private Optional<Boolean> syncCommits = Optional.empty();
    private Optional<Boolean> ackOnError = Optional.empty();
    private Optional<Boolean> batchListener = Optional.empty();
    private Optional<MessageConverter> messageConverter = Optional.empty();
    private Optional<ErrorHandler> errorHandler = Optional.empty();
    private Optional<BatchErrorHandler> batchErrorHandler = Optional.empty();


    SpringKafkaConsumerFactoryBuilder(Map<String, Object> props, EnvironmentProvider environmentProvider, Optional<ObjectMapper> objectMapper) {
        super(props, environmentProvider);
        this.objectMapper = objectMapper;
        readProperties();
    }

    private void readProperties() {
        readProperty(POLL_TIMEOUT_CONFIG, Duration::parse, this::withPollTimeout);
        readProperty(ACK_MODE_CONFIG, AckMode::valueOf, this::withAckMode);
        readProperty(CONCURRENCY_CONFIG, Integer::valueOf, this::withConcurrency);
        readProperty(SYNC_COMMITS_CONFIG, Boolean::valueOf, this::withSyncCommits);
        readProperty(ACK_ON_ERROR_CONFIG, Boolean::valueOf, this::withAckOnError);
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
        withAutoCommit(value == null);
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
     * Set whether or not to call consumer.commitSync() or commitAsync() when the
     * container is responsible for commits. Default true. See
     * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
     * writing, async commits are not entirely reliable.
     * @param value true to use commitSync().
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withSyncCommits(Boolean value) {
        this.syncCommits = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set whether or not the container should commit offsets (ack messages) where the
     * listener throws exceptions. This works in conjunction with {@link #ackMode} and is
     * effective only when the kafka property {@code enable.auto.commit} is {@code false};
     * it is not applicable to manual ack modes. When this property is set to {@code true}
     * (the default), all messages handled will have their offset committed. When set to
     * {@code false}, offsets will be committed only for successfully handled messages.
     * Manual acks will always be applied. Bear in mind that, if the next message is
     * successfully handled, its offset will be committed, effectively committing the
     * offset of the failed message anyway, so this option has limited applicability.
     * Perhaps useful for a component that starts throwing exceptions consistently;
     * allowing it to resume when restarted from the last successfully processed message.
     * <p>
     * Does not apply when transactions are used - in that case, whether or not the
     * offsets are sent to the transaction depends on whether the transaction is committed
     * or rolled back. If a listener throws an exception, the transaction will normally
     * be rolled back unless an error handler is provided that handles the error and
     * exits normally; in which case the offsets are sent to the transaction and the
     * transaction is committed.
     * @param value whether the container should acknowledge messages that throw
     * exceptions.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withAckOnError(Boolean value) {
        this.ackOnError = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set to true if this endpoint should create a batch listener.
     * @param value true for a batch listener.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withBatchListener(Boolean value) {
        this.batchListener = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set the message converter to use if dynamic argument type matching is needed.
     * @param value the converter.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withMessageConverter(MessageConverter value) {
        this.messageConverter = Optional.ofNullable(value);
        return self();
    }

    /**
     * Configures BytesJsonMessageConverter to use with provided ObjectMapper
     * @param objectMapper
     * @return
     */
    public<K2> SpringKafkaConsumerFactoryBuilder<K2, Object> withJsonMessageConverter(Class<? extends Deserializer<K2>> keyDeSer, ObjectMapper objectMapper) {
        return (SpringKafkaConsumerFactoryBuilder) withMessageConverter(new BytesJsonMessageConverter(objectMapper))
            .withDeserializers(keyDeSer, BytesDeserializer.class);
    }

    /**
     * Configures BytesJsonMessageConverter to use with default ObjectMapper
     * @return
     */
    public<K2> SpringKafkaConsumerFactoryBuilder<K2, Object> withJsonMessageConverter(Class<? extends Deserializer<K2>> keyDeSer) {
        return (SpringKafkaConsumerFactoryBuilder) withMessageConverter(objectMapper.map(BytesJsonMessageConverter::new).orElse(new BytesJsonMessageConverter()))
            .withDeserializers(keyDeSer, BytesDeserializer.class);
    }

    /**
     * Specify callback to customize {@link ContainerProperties}
     * @param value - callback
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withContainerPropertiesCustomizer(Consumer<ContainerProperties> value) {
        this.containerPropertiesCustomizer = Optional.ofNullable(value);
        return self();
    }

    /**
     * Specify callback to customize {@link KafkaListenerContainerFactory}
     * @param value - callback
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withFactoryCustomizer(Consumer<KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>>> value) {
        this.factoryCustomizer = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set the error handler to call when the listener throws an exception.
     * @param value the error handler.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withErrorHandler(ErrorHandler value) {
        this.errorHandler = Optional.ofNullable(value);
        return self();
    }

    /**
     * Set the batch error handler to call when the listener throws an exception.
     * @param value the error handler.
     */
    public SpringKafkaConsumerFactoryBuilder<K, V> withBatchErrorHandler(BatchErrorHandler value) {
        this.batchErrorHandler = Optional.ofNullable(value);
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
        final ContainerProperties containerProperties = factory.getContainerProperties();

        pollTimeout.map(Duration::toMillis).ifPresent(containerProperties::setPollTimeout);
        ackMode.ifPresent(containerProperties::setAckMode);
        syncCommits.ifPresent(containerProperties::setSyncCommits);
        ackOnError.ifPresent(containerProperties::setAckOnError);

        concurrency.ifPresent(factory::setConcurrency);
        batchListener.ifPresent(factory::setBatchListener);
        messageConverter.ifPresent(factory::setMessageConverter);
        errorHandler.ifPresent(factory::setErrorHandler);
        batchErrorHandler.ifPresent(factory::setBatchErrorHandler);

        containerPropertiesCustomizer.ifPresent(c -> c.accept(containerProperties));
        factoryCustomizer.ifPresent(c -> c.accept(factory));

        return factory;
    }
}
