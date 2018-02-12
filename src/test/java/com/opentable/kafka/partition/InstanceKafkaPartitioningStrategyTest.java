package com.opentable.kafka.partition;


import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Multimap;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;
import com.opentable.kafka.embedded.EmbeddedKafkaRule;


public class InstanceKafkaPartitioningStrategyTest {
    private static final String TOPIC = "foo-foo";

    @Rule
    public final EmbeddedKafkaRule ekr = new EmbeddedKafkaBuilder()
            .withTopics(TOPIC)
            .nPartitions(3)
            .rule();

    private InstanceKafkaPartitioningStrategy instanceKafkaPartitioningStrategy;

    @Before
    public void before() throws Exception {
        final String brokerList = ekr.getBroker().getKafkaBrokerConnect();
        BrokerConfig brokerConfig = new BrokerConfig(brokerList, TOPIC, true);
        instanceKafkaPartitioningStrategy = new InstanceKafkaPartitioningStrategy(brokerConfig, null, new EvenlyDistributedInstanceToPartitionMapper());
    }

    @Test
    public void testPartitionDetermination() {
        // Embedded kafka has only one partition, default assumptions are 1 instance number, 1 instance count
        Assert.assertEquals(3, instanceKafkaPartitioningStrategy.getPartitionList().size());
        Assertions.assertThat(instanceKafkaPartitioningStrategy.getPartitionList()).contains(0, 1, 2);
        // Now try an assigned number
        int partitionCount = 29;
        int instanceCount = 3;
        Multimap<Integer, Integer> map = instanceKafkaPartitioningStrategy.getInstanceToPartitionMap(partitionCount, instanceCount);
        Assert.assertEquals(instanceCount, map.keySet().size());
        Assertions.assertThat(map.get(0)).contains(0, 18, 3, 21, 6, 21, 6, 24, 9, 27, 12, 15);
        Assertions.assertThat(map.get(1)).contains(16, 1, 19, 4, 22, 7, 25, 10, 28, 13);
        Assertions.assertThat(map.get(2)).contains(17, 2, 20, 5, 23, 8, 26, 11, 14);
        // All partitions included, and only once
        Set<Integer> collection = new HashSet<>(map.values());

        Assert.assertEquals(partitionCount, collection.size());
        Assert.assertEquals(collection, IntStream.range(0, partitionCount).boxed().collect(Collectors.toSet()));

    }
}