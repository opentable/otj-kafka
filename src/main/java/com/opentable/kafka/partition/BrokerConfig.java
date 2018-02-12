package com.opentable.kafka.partition;

/**
    Use this to create a bean providing the basic configuration needed
 */
public class BrokerConfig {
    /**
     * Comma delimited list of brokers
     */
    private final String brokerList;
    /**
     * topic
     */
    private final String topic; // topic
    /**
     * is it enabled? Should be true for "normal" ops, false for when testing without kafka
     */
    private final boolean enabled;

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
