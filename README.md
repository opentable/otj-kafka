Kafka integrations and convenience libraries.

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
