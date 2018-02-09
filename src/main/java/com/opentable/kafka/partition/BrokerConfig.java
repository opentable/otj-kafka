package com.opentable.kafka.partition;

/**
    Use this to create a bean providing the basic configuration needed
 */
public class BrokerConfig {
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
