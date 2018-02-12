package com.opentable.kafka.partition;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * This is the standard immplementation, which simply distributes the partitions evenly (but consistently)
 * across the instances.
 */
public class EvenlyDistributedInstanceToPartitionMapper implements InstanceToPartitionMapper{

    @Override
    public Multimap<Integer, Integer> instanceToPartitionMap(final int partitionCount, final int instanceCount) {
        final Multimap<Integer, Integer> map = HashMultimap.create();
        for (int partition = 0; partition < partitionCount; partition++) {
            int assignedInstance = partition % instanceCount;
            map.put(assignedInstance, partition);
        }
        return map;
    }
}
