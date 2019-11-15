package com.opentable.kafka.session;

import java.util.List;

public class TestResult {
    private final List<ConsumedEvent<String>> events;
    private final List<ConsumerTask<String>> consumerTaskList;
    private final ProducerTask producerTask;

    public TestResult(final List<ConsumerTask<String>> consumerTaskList, final List<ConsumedEvent<String>> events, final ProducerTask producerTask) {
        this.events = events;
        this.consumerTaskList = consumerTaskList;
        this.producerTask = producerTask;
    }

    public List<ConsumerTask<String>> getConsumerTaskList() {
        return consumerTaskList;
    }

    public List<ConsumedEvent<String>> getEvents() {
        return events;
    }

    public ProducerTask getProducerTask() {
        return producerTask;
    }
}
