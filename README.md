Kafka integrations and convenience libraries.

Kafka Builders
--------------

We provide builders for KafkaConsumer and KafkaProducer.
Why would you use them?

* Spring friendly
* Fluent API, to make most Kafka properties a little more readable and strongly typed
* Integration with metrics - automatically forwards Kafka metrics to graphite
* OTL logging - on a sampled basis.
* Support for injecting raw properties via Spring configuration files.

Usage:

Add `@InjectKafkaBuilderBeans` to any Configuration class. You'll now have two beans in your context:

* KafkaConsumerBuilderFactoryBean
* KafkaProducerBuilderFactoryBean

You may inject them into any class, and use them. The general idiom they follow looks something like this

```
 final Consumer<byte[], ABEvent> consumer = kafkaConsumerBuilderFactoryBean.<byte[],ABEvent>
                builder("myuniquenameperconsumerpermachine) // name must be unique per machine. This works because of the deterministic setup
                .withBootstrapServers(Arrays.asList(brokerList().split(",")))
                .withAutoCommit(false)
                .withClientId(makeClientId(topicPartition))
                .withGroupId(getKafkaGroupId())
                .withAutoOffsetReset(autoOffsetResetMode.getType())
                .withDeserializers(keyDeserializer, abEventDeserializer)
                .build();
                ;

```

This will build the Kafka producer/consumer using the following logic:

* If there's any ot.kafka.myuniquenameperconsumerpermachine namespaced configuration properties, use these. They
will take precedence over the fluent api
* We preassign client-id in the bean as name.(serviceInfo.name).(incrementingNumber). You may override it as shown above, if desired.
* Add in any thing specified in the fluent api
* Wire in metrics and logging. To disable these use `disableLogging()` and/or `disableMetrics()`. The logging only
kicks in once per 10 seconds, but you may change this rate via `withLoggingSampleRate()` (for example setting to 10 will make it rate limit
once per second, changing to 100 will have a rate limit of 10 per second, etc) - mind our logging cluster though!
* Return the Kafka consumer/producer (we return the interface type Consumer/Producer instead of the implementations KafkaConsumer/KafkaProducer)

The BuilderFactoryBean is thread safe, but the underlying Builder is NOT.

Offset Metrics
--------------

We provide a class `OffsetMetrics` that adds metrics to a
`MetricRegistry` instrumenting the size (maximum offset) of given topics
along with the offset and lag (size minus offset) of consumer groups'
consumption of those topics.

Use is simple.  If using Spring, simply add a new `@Bean`.  Here is an
example from chat.

    @Bean
    OffsetMetrics offsetMetrics(
            final MetricRegistry metricRegistry,
            final KafkaConnectionConfiguration connConfig) {
        final String metricPrefix = "chat.offset-metrics";
        final String groupId = ChatConstants.APP_ID;
        return OffsetMetrics
                .builder(
                    metricPrefix, metricRegistry,
                    groupId, connConfig.getConnectString()
                )
                .addTopics(
                    connConfig.getRequestTopic(),
                    connConfig.getMessageTopic()
                )
                .build();
    }

`OffsetMetrics` registers its metrics on startup, so it requires a reference
to a `MetricRegistry` and a prefix for the metrics it'll register. Metrics
will be registered under the namespace `kafka.<prefix>.<topic>". The prefix
cannot contain a period.

If not using Spring, just be sure to call `start` after construction, and
`stop` before disposal or shutdown.

The set of instrumented metrics from one instance is complete.  That is,
you don't need to run more than one of these.  Consequently, you may
want to run it in a manager / sidecar sort of service.  Or you may want
to selectively run it from only one of your service instances.  Or you
might just not worry about it, and read off the metrics from one
instance only.

For more detail, see the extensive Javadoc documentation on the
`OffsetMetrics` class itself, and the methods available to you on its
builder.
