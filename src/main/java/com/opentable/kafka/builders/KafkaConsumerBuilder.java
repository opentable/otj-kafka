package com.opentable.kafka.builders;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import com.opentable.kafka.logging.LoggingConsumerInterceptor;
import com.opentable.kafka.logging.LoggingInterceptorConfig;

public class KafkaConsumerBuilder<K, V> extends KafkaBuilder<KafkaConsumerBuilder<K, V>> {

    KafkaConsumerBuilder(Properties prop) {
        super(prop);
        withLogging();
    }

    @Override
    KafkaConsumerBuilder<K, V> self() {
        return this;
    }

    public static  KafkaConsumerBuilder<?, ?> builder() {
        return new KafkaConsumerBuilder<>(new Properties());
    }

    public static KafkaConsumerBuilder<?, ?> builder(Properties props) {
        return new KafkaConsumerBuilder<>(props);
    }

    public KafkaConsumerBuilder<K, V> withLogging() {
        setListPropItem(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> disableLogging() {
        removeListPropItem(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingConsumerInterceptor.class.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withLoggingSampleRate(Double rate) {
        return withProperty(LoggingInterceptorConfig.SAMPLE_RATE_PCT_CONFIG, rate);
    }

    public KafkaConsumerBuilder<K, V> withInterceptor(Class<? extends ConsumerInterceptor<K, V>> clazz) {
        setListPropItem(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, clazz.getName());
        return this;
    }

    public KafkaConsumerBuilder<K, V> withGroupId(String val) {
        return withProperty(ConsumerConfig.GROUP_ID_CONFIG, val);
    }

    public KafkaConsumerBuilder<K, V> withOffsetReset(AutoOffsetResetType val) {
        return withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, val.value);
    }

    public KafkaConsumerBuilder<K, V> withMaxPollRecords(int val) {
        return withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, val);
    }

    public <K2, V2> KafkaConsumerBuilder<K2, V2> withDeserializers(Class<? extends Deserializer<K2>> keyDeSer, Class<? extends Deserializer<V2>> valDeSer) {
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSer);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeSer);
        return new KafkaConsumerBuilder<>(prop);
    }

    public KafkaConsumerBuilder<K, V> withAutoCommit(boolean val) {
        return withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, val);
    }

    public KafkaConsumerBuilder<K, V> withSessionTimeoutMs(Duration val) {
        return withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, val.toMillis());
    }

    public KafkaConsumerBuilder<K, V> withMaxPartitionFetchBytes(int val) {
        return withProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, val);
    }

    public KafkaConsumerBuilder<K, V> withPollInterval(Duration val) {
        return withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, val.toMillis());
    }

    public KafkaConsumer<K, V> build() {
        // TODO: add checks here
        return new KafkaConsumer<>(prop);
    }

    public <K2, V2> KafkaConsumer<K2, V2> build(Deserializer<K2> keyDeSer, Deserializer<V2> valDeSer) {
        // TODO: add checks here
        return new KafkaConsumer<>(prop, keyDeSer, valDeSer);
    }

    public enum AutoOffsetResetType {
        Latest("latest"), Earliest("earliest"), None("none");
        final String value;
        AutoOffsetResetType(String value) {
            this.value = value;
        }
    }
}
