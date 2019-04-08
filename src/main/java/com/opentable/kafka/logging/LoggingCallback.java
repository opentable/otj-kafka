package com.opentable.kafka.logging;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class LoggingCallback<K, V> implements Callback {

    private final Callback callback;
    private final ProducerRecord<K, V> record;

    public LoggingCallback(Callback callback, ProducerRecord<K, V> record) {
        this.callback = callback;
        this.record = record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            // on error
        }
        if (callback != null) {
            callback.onCompletion(metadata, exception);
        }
    }

}
