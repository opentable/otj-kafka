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
package com.opentable.kafka.util;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.common.base.Verify;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

@SuppressWarnings("PMD.CloseResource")
public class OffsetMonitor implements Closeable {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final String consumerGroupId;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final AdminClient adminClient;

    /**
     * @param  consumerNamespace Namespace the consumer created by this class; for example, pass your application name.
     * @param brokerList list of kafka brokers, in the traditional comma delmited fashion
     */
    public OffsetMonitor(final String consumerNamespace, final String brokerList) {
        consumerGroupId = consumerNamespace + "-offset-monitor";
        consumer = makeConsumer(brokerList);
        adminClient = makeAdminClient(brokerList);
    }

    private KafkaConsumer<byte[], byte[]>  makeConsumer(final String brokerList) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
        final Deserializer<byte[]> deser = Serdes.ByteArray().deserializer();
        return new KafkaConsumer<>(props, deser, deser);
    }

    private AdminClient makeAdminClient(final String brokerList) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return AdminClient.create(props);
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void close() {
        try {
            consumer.close();
        } finally {
            adminClient.close();
        }
    }

    /**
     * Find size (maximum offset) for each partition on topic.
     *
     * <p>
     * If the topic does not exist, the returned map will be empty.
     *
     * @param topic the topic for which to fetch partition sizes (maximum offsets)
     * @return map of partition of size (maximum offset)
     */
    public Map<Integer, Long> getTopicSizes(final String topic) {
        final Collection<PartitionInfo> partInfo = consumer.partitionsFor(topic);
        if (partInfo == null) {
            // Topic does not exist.
            return Collections.emptyMap();
        }
        final Collection<TopicPartition> parts = partInfo
                .stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toSet());
        final Map<TopicPartition, Long> sizes = consumer.endOffsets(parts);
        return sizes
                .entrySet()
                .stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().partition(),
                                Map.Entry::getValue
                        )
                );
    }

    /**
     * Find current offset for each partition on topic being consumed by consumer group.
     *
     * <p>
     * If the consumer group does not exist, the returned map will be empty.
     * If the consumer group exists, but it is not consuming the topic, the returned map will be empty.
     *
     * @param groupId consumer group ID
     * @param topic topic the group is consuming
     * @return map of partition to offset
     */
    public Map<Integer, Long> getGroupOffsets(final String groupId, final String topic) {

        try {
            return adminClient.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().topic().equals(topic))
                    .collect(
                            Collectors.toMap(
                                    k -> k.getKey().partition(),
                                    v -> v.getValue().offset()
                            )
                    );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Was interrupted"); //NOPMD
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(final String[] args) {
        final String brokerList = args[0];
        final String groupId = args[1];
        final String topic = args[2];
        try (OffsetMonitor mon = new OffsetMonitor("ot-kafka-util-om-cli", brokerList)) {
            final Map<Integer, Long> topicSizes = mon.getTopicSizes(topic);
            final Map<Integer, Long> groupOffsets = mon.getGroupOffsets(groupId, topic);
            Verify.verify(topicSizes.keySet().equals(groupOffsets.keySet()));
            System.out.printf("# partition size offset lag%n");
            topicSizes
                    .keySet()
                    .stream()
                    .sorted()
                    .forEach(part -> {
                        final long size = topicSizes.get(part);
                        final long off = groupOffsets.get(part);
                        final long lag = size - off;
                        System.out.printf("%d %d %d %d%n", part, size, off, lag);
                    });
        }
    }
}
