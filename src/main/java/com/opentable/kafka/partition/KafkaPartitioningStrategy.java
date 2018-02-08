package com.opentable.kafka.partition;

import java.util.List;

public interface KafkaPartitioningStrategy {
    List<Integer> getPartitionList();
}
