otj-kafka changelog
===================
5.2.9
-----
* updated to use jetty 10.0.12 and sb2.7.15 (pom 374)

5.2.7
-----
* Add support for compression in DSL.

5.2.6
-----
* POM 344
* OTPL-6894 minor tweaks

5.2.5
-----
* recompile to compensate for binary compatibility break in bucket4j

5.2.4
-----
* Fix some flaky tests due to  races in `EmbeddedKafkaBroker`

5.2.3
-----
* Parent POM-328 [changes see here]( https://github.com/opentable/otj-parent/blob/master/CHANGELOG.md#328)

5.2.1
----
* No changes, was testing Nexus

5.2.0
------
* Added a supplier() method. If you think you'll need to create the same consumer or producer
multiple time, this works nicely.
* tests added.
* Recompile for Spring 5.2 and Kafka 2.4.1

3.2.2
-----
* Hierarchy of builders was refactored to use inheritance. The properties defined in the `KafkaBaseBuilder` visible on child builders.
* Added support for spring-kafka abstractions:
The bean `KafkaFactoryBuilderFactoryBean` can be used to construct `ProducerFactory<K, V>` and `KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>>` using fluent API.

3.2.1
-----
* Move LogRestoreListener to new module `otj-kafka-streams`. If you are using this, you'll need
to pull in the dependency declaration

```$xslt
<dependency>
    <groupId>com.opentable.components</groupId>
    <artifactId>otj-kafka-streams</artifactId>
</dependency>
```

* Add experimental support for Kafka Streams via builder. This is a work in progress.
* Rearrange logging logic to not overlog, and add to otl the opentracing headers.
* Add `otj-kafka-mirrormaker` module with a utility for dynamically changing topic names when mirroring them.

3.2.0
-----
* Add spring factory bean KafkaBuilderFactoryBean
 This you can normally inject into your context using `@InjectKafkaBuilderBeans`.

The builder provides:
* Fluent API for all common configuration settings, plus manual override by property.
* Optional support for property file configuration, which takes precedence.
* Optional (but wired by default) logging interceptors that log a sample of your producer/consumer
traffic using a new standardized OTL. The default rate is 1 per every 10 seconds, but you can
override this rate from the fluent API. You may also disable by calling noLogging() in the fluent api. You also can configure to log percent of the records.
* Optional (but wired by default) metrics interceptors that wire up all of kafka's very extensive metrics
and forward to graphite.

Detailed instructions for usage are in the README.MD.

* Move EmbeddedKafka and Embedded Zookeeper to `otj-kafka-tests` module. If you use these you may
still access them via the following dependency declaration

```$xslt
<dependency>
    <groupId>com.opentable.components</groupId>
    <artifactId>otj-kafka-tests</artifactId>
</dependency>
```

(probably scoped as test scope)

3.1.3
-----
* Move test from otj-logging. This breaks a cyclic dependency.

3.1.2
-----
* Recompile for DAG

3.1.1
-----
* Recompile for DW 4
3.1.0
-----
* Uses otj-metrics 3.0

3.0.0
-----
* **Requires** and supports Kafka 2.0.0

If you want to use Kafka 1.x you need to use an older library version.

2.8.4
-----
* Fix possible NPE in OffsetMetrics

2.8.3
-----
* OffsetMetrics no longer allows metricsPrefix containing period and is always prepended with "kafka."
* POM update to 152

2.8.2
-----

* improve embedded kafka broker startup

2.8.1
-----

* minor 1.1.0 bugfix

2.8.0
-----

* kafka 1.1.0!

2.7.1
-----

* dramatically improve EmbeddedKafka speed for single-broker case

2.7.0
-----

* Add a LogProgressRestoreListener to watch Kafka restore

2.6.1
-----
* Spring Boot 2/5.0.4

2.6.0
-----
* Add ManualKafkaPartitioningStrategy.

2.5.0
-----

* offset metrics now supports self-offset management

2.4.2
-----

* limit kafka offset metrics logging

2.4.1
-----

* improved embedded kafka startup time (offset partitions: 50 -> 1)

2.4.0
-----

* kafka 1.0.0

2.3.0
-----

* added offset monitoring utilities
* embedded kafka broker will now ready itself

2.2.0
-----

* added delegate creator helper method for key serializer instance

2.1.0
-----

* added JsonSerde for general Jackson/JSON/Java object mapping

pre 2.1.0
---------

Ancient history.
