package com.opentable.kafka.builders;

import javax.inject.Inject;

import com.opentable.service.AppInfo;
import com.opentable.service.EnvInfo;

/**
 * Standard provider for Spring, wired in KafkaBuilderConfiguration
 */
public class AppInfoEnvironmentProvider implements EnvironmentProvider {
    private final AppInfo appInfo;
    private final EnvInfo envInfo;

    @Inject
    public AppInfoEnvironmentProvider(AppInfo appInfo) {
        this.appInfo = appInfo;
        this.envInfo = appInfo.getEnvInfo();
    }

    @Override
    public String getReferringHost() {
        return appInfo.getTaskHost();
    }

    @Override
    public Integer getReferringInstanceNumber() {
        return appInfo.getInstanceNumber();
    }

    @Override
    public String getEnvironment() {
        return envInfo == null ? null : envInfo.getEnvironment();
    }

    @Override
    public String getEnvironmentFlavor() {
        return envInfo == null ? null : envInfo.getFlavor();
    }
}
