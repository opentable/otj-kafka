package com.opentable.kafka.partition;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.service.AppInfo;

/**
 * Uses a consistent manual partitioning strategy. On Mesos, therefore the same instance # will always be
 * assigned the same partitions assuming
 * - the total number of partitions in topic hasn't changed
 * - the total instance count has not changed.
 *
 * NOTE: Normally the right thing to do is to use automatic rebalancing (Kafka default), which is a proven
 * scalable strategy that elastically scales. Use this class only if there is a reason
 */
public class ManualKafkaPartitioningStrategy implements KafkaPartitioningStrategy {
        private static final Logger LOG = LoggerFactory.getLogger(ManualKafkaPartitioningStrategy.class);
        private final BrokerConfig brokerConfig;
        private final AppInfo appinfo;
        private final ImmutableList<Integer> partitionList;

        @Inject
        public ManualKafkaPartitioningStrategy(
                final BrokerConfig brokerConfig,
                final AppInfo appInfo
                ) throws ExecutionException, InterruptedException {

            this.brokerConfig = brokerConfig;
            final int partitionCount = numberOfPartitions(brokerConfig.getTopic(), brokerConfig.getBrokerList());
            this.appinfo = appInfo;
            final int instanceNumber = calculateInstanceNumber();
            final int instanceCount = calculateTotalInstances();
            // If kafka is enabled and instance > partition probably a muck up.
            if (((instanceCount > partitionCount) && (brokerConfig.isEnabled()))) {
                throw new IllegalStateException("More instances than partitions - probably a misconfiguration?");
            }
            // Calculate distribution of instances to partitions
            Multimap<Integer, Integer> map = getInstanceToPartitionMap(partitionCount, instanceCount);
            this.partitionList = ImmutableList.copyOf(map.get(instanceNumber - 1));
            LOG.info("Partition count {}, instance number {}, instance count {} partitions to assign {}", partitionCount, instanceNumber, instanceCount, partitionList);
        }

        @VisibleForTesting
        Multimap<Integer, Integer> getInstanceToPartitionMap(final int partitionCount, final int instanceCount) {
            final Multimap<Integer, Integer> map = HashMultimap.create();
            for (int partition = 0; partition < partitionCount; partition++) {
                int assignedInstance = partition % instanceCount;
                map.put(assignedInstance, partition);
            }
            return map;
        }

        // On local or non mesos always return 1
        private int calculateTotalInstances() {
            return appinfo != null && appinfo.getInstanceCount() != null ? appinfo.getInstanceCount(): 1;
        }

        private int calculateInstanceNumber() {
            return appinfo != null && appinfo.getInstanceNumber() != null ? appinfo.getInstanceNumber() : 1;
        }

    /*
        Use Kafka admin api to determine the number of partitions for this topic
    */
    private int numberOfPartitions(final String topic, final String brokerList) throws ExecutionException, InterruptedException {
            if (!brokerConfig.isEnabled()) {
                return 1;
            }
            final AdminClient client = makeAdminClient(brokerList);
            final DescribeTopicsResult result = client.describeTopics(Lists.newArrayList(topic));
            final TopicDescription topicDescription = result.all().get().get(topic);
            return topicDescription.partitions().size();
    }

    private AdminClient makeAdminClient(final String brokerList) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return AdminClient.create(props);
    }

    /**
     * Return the Partitions assigned to this instance
     */
    @Override
    public List<Integer> getPartitionList() {
        return partitionList;
    }

    /**
        Use this to create a bean providing the basic configuration needed
     */
    public static class BrokerConfig {
        private final String brokerList; // comma delimited list of brokers
        private final String topic; // topic
        private final boolean enabled; // is it enabled? Should be true for "normal" ops, false for when testing without kafka

        public BrokerConfig(final String brokerList, final String topic, final boolean kafkaEnabled) {
            this.brokerList = brokerList;
            this.topic = topic;
            this.enabled = kafkaEnabled;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public String getTopic() {
            return topic;
        }

        public String getBrokerList() {
            return brokerList;
        }
    }
}
