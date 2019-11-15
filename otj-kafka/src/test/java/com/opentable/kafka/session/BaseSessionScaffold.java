package com.opentable.kafka.session;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.kafka.builders.KafkaConsumerBaseBuilder;
import com.opentable.kafka.builders.KafkaConsumerBuilder;
import com.opentable.kafka.builders.KafkaProducerBuilder;
import com.opentable.kafka.builders.SettableEnvironmentProvider;
import com.opentable.kafka.embedded.EmbeddedKafkaBroker;

// Class provides a broker, eventbus, before and after semantics, and basic builders
public abstract class BaseSessionScaffold {
    protected static final Logger LOG = LoggerFactory.getLogger(BaseSessionScaffold.class);

    protected static final String TEST_TOPIC = "testing";

    protected EmbeddedKafkaBroker embeddedKafkaBroker;
    protected EventBus<String> eventBus;

    public abstract BiConsumer<Runnable, Consumer<Integer, String>> getConsumerRecoveryLambda();
    @Before
    public void before() {
        eventBus = new EventBus<>();
        // Create the embedded kafka, precreate topic, and set partitions to 32.
        embeddedKafkaBroker =  new EmbeddedKafkaBroker(Collections.singletonList(TEST_TOPIC), true, 32);
        embeddedKafkaBroker.start();
    }

    @After
    public void after() {
        embeddedKafkaBroker.close();
    }

    protected ProducerTask getProducerTask(final Producer<Integer, String> producer, Integer maxRecords) {
        return new ProducerTask(producer, TEST_TOPIC, maxRecords);
    }

    protected ConsumerTask<String> getConsumerTask(final Consumer<Integer, String> consumer, Integer sleepPoint) {
        return new ConsumerTask<>(eventBus, consumer, TEST_TOPIC, sleepPoint, getConsumerRecoveryLambda());
    }

    protected Producer<Integer, String> producer() {
        return producerBuilder()
                .withSerializers(IntegerSerializer.class, StringSerializer.class)
                .withBootstrapServer(embeddedKafkaBroker.getKafkaBrokerConnect())
                .withClientId(UUID.randomUUID().toString()) // each producer is unique
                .disableLogging()
                .disableMetrics()
                .build();
    }

    protected Consumer<Integer, String> consumer(int consumerNumber, String groupId) {
        return consumerBuilder()
                .withDeserializers(IntegerDeserializer.class, StringDeserializer.class)
                .withGroupId(groupId) // force new offset management
                .withBootstrapServer(embeddedKafkaBroker.getKafkaBrokerConnect())
                .withAutoCommit(true)
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
                .withClientId("consumer-" + consumerNumber) // each is tied to the consumer number
                .withMaxPollRecords(1) // 1 each time we poll makes book keeping much easier.
                // Seems to take in the output log, and should cause rebalance shortly after 10s
                .withSessionTimeoutMs(Duration.ofSeconds(10))
                .withProperty("max.poll.interval.ms", (int) Duration.ofSeconds(11).toMillis())
                .withAutoOffsetReset(KafkaConsumerBaseBuilder.AutoOffsetResetType.Earliest)
                .disableLogging()
                .disableMetrics()
                .build();
    }

    public KafkaConsumerBuilder<Integer, String> consumerBuilder() {
        return new KafkaConsumerBuilder<>(new HashMap<>(), new SettableEnvironmentProvider("","",1,"",""));
    }

    public KafkaProducerBuilder<Integer, String> producerBuilder() {
        return new KafkaProducerBuilder<>(new HashMap<>(), new SettableEnvironmentProvider("", "", 1, "", ""));
    }
}
