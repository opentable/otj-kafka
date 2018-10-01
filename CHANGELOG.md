otj-kafka changelog
===================

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

* added producer creator helper method for key serializer instance

2.1.0
-----

* added JsonSerde for general Jackson/JSON/Java object mapping

pre 2.1.0
---------

Ancient history.
