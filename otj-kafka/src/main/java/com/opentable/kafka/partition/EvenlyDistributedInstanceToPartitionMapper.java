/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
