package com.opentable.kafka.logging;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingCallback<K, V> implements Callback {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingKafkaProducer.class);

    private final Callback callback;
    private final ProducerRecord<K, V> record;
    private final String clientId;

    public LoggingCallback(Callback callback, ProducerRecord<K, V> record, String clientId) {
        this.callback = callback;
        this.record = record;
        this.clientId = clientId;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            // on error
        } else {
            LoggingUtils.trace(LOG, clientId, record);
        }
        if (callback != null) {
            callback.onCompletion(metadata, exception);
        }
    }

}
