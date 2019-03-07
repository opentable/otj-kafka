package com.opentable.kafka.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.ws.rs.core.HttpHeaders;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.logging.otl.HttpV1;

public class DelegatingConsumer<K,V> implements Consumer<K,V> {

    // This is skeleton - see poll for an example
    private final static Logger LOG = LoggerFactory.getLogger(DelegatingConsumer.class);
    private final KafkaConsumer<K, V> delegate;
    private final MetricRegistry metricRegistry;

    // One of many metrics
    private final Timer pollTime;
    private final Meter recordsPolled;

    public DelegatingConsumer(KafkaConsumer<K, V> delegate, MetricRegistry metricRegistry) {
        this.delegate = delegate;
        this.metricRegistry = metricRegistry;
        this.pollTime = metricRegistry.timer("poll.time");
        this.recordsPolled = metricRegistry.meter("poll.records");
    }
    @Override
    public Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    @Override
    public Set<String> subscription() {
        return delegate.subscription();
    }

    @Override
    public void subscribe(final Collection<String> topics) {
        delegate.subscribe(topics);
    }

    @Override
    public void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
        delegate.subscribe(topics, callback);
    }

    @Override
    public void assign(final Collection<TopicPartition> partitions) {
        delegate.assign(partitions);
    }

    @Override
    public void subscribe(final Pattern pattern, final ConsumerRebalanceListener callback) {
        delegate.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(final Pattern pattern) {
        delegate.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(final long timeout) {
        try (Timer.Context c = pollTime.time()) {
            return delegate.poll(timeout);
        }
    }

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        try (Timer.Context c = pollTime.time()) {
            ConsumerRecords<K,V> records = delegate.poll(timeout);
            int numberOfRecordsReturned = records.count();
            recordsPolled.mark(numberOfRecordsReturned); // track number of records read
            //records.partitions() - track topics and partitions
            // Possibly too expensive to iterate as well to count all the bytes
            // This is not the otl or otls you might design, it's just an example
            // Of the power of OTLS
            if (ratelimit()) {
                final HttpV1.HttpV1Builder builder = HttpV1
                        .HttpV1()
                        .acceptLanguage("language")
                        .incoming(false)
                        .status(200)
                        .responseSize(500)
                        .bodySize(500)
                        .method("GET")
                        .url("url")
                        .userAgent("agent")
                        .referer("referer")
                        .duration(500000L);
                // Instead of this you might log something like number of items read, topics, etc
                // so think of this conceptually rathher than literally
                LOG.debug(builder.log(), "Response code {}, uri {}","blah", "blah");
            }
            return records;
        }
    }

    // We need to ratelimit because we don't want to excessively log.
    public boolean ratelimit() {
        return true; // replace with bucket4j token limited
    }
    @Override
    public void commitSync() {
        delegate.commitSync();
    }

    @Override
    public void commitSync(final Duration timeout) {
        delegate.commitSync(timeout);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
    }

    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        delegate.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {

    }

    @Override
    public void seek(final TopicPartition partition, final long offset) {

    }

    @Override
    public void seekToBeginning(final Collection<TopicPartition> partitions) {

    }

    @Override
    public void seekToEnd(final Collection<TopicPartition> partitions) {

    }

    @Override
    public long position(final TopicPartition partition) {
        return 0;
    }

    @Override
    public long position(final TopicPartition partition, final Duration timeout) {
        return 0;
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(final Collection<TopicPartition> partitions) {

    }

    @Override
    public void resume(final Collection<TopicPartition> partitions) {

    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
        return null;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(final long timeout, final TimeUnit unit) {
        delegate.close(timeout, unit);
    }

    @Override
    public void close(final Duration timeout) {
        delegate.close(timeout);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }



}
