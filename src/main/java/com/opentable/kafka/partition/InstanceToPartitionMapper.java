package com.opentable.kafka.partition;

import com.google.common.collect.Multimap;

/**
 * Given a Kafka partition count and Mesos instance count, provide mapping of instance to partition(s).
 */
@FunctionalInterface
public interface InstanceToPartitionMapper {
    Multimap<Integer, Integer> instanceToPartitionMap(final int partitionCount, final int instanceCount);
}
