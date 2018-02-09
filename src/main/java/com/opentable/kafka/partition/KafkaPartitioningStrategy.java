package com.opentable.kafka.partition;

import java.util.List;

/**
 * Provide a List of Kafka partitions that you "own"
 */
public interface KafkaPartitioningStrategy {
    List<Integer> getPartitionList();
}
