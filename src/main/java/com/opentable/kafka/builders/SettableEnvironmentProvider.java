package com.opentable.kafka.builders;

/**
 * For non spring environments, allows an easy way to inject these values.
 */
public class SettableEnvironmentProvider implements EnvironmentProvider {
    private final String referringService;
    private final String referringHost;
    private final Integer referringInstanceNumber;
    private final String environment;
    private final String flavor;

    public SettableEnvironmentProvider(final String referringService, final String referringHost, final Integer referringInstanceNumber, final String environment, final String flavor) {
        this.referringService = referringService;
        this.referringHost = referringHost;
        this.referringInstanceNumber = referringInstanceNumber;
        this.environment = environment;
        this.flavor = flavor;
    }

    @Override
    public String getReferringService() {
        return referringService;
    }

    @Override
    public String getReferringHost() {
        return referringHost;
    }

    @Override
    public Integer getReferringInstanceNumber() {
        return referringInstanceNumber;
    }

    @Override
    public String getEnvironment() {
        return environment;
    }

    @Override
    public String getEnvironmentFlavor() {
        return flavor;
    }
}
