package com.opentable.kafka.builders;

import com.opentable.logging.CommonLogHolder;

/**
 * Normally a thin wrapper around AppInfo/Envinfo in Spring, this allows
 * alternative implementationd
 */
public interface EnvironmentProvider {
    default String getReferringService() {
        return CommonLogHolder.getServiceType();
    }
    String getReferringHost();
    Integer getReferringInstanceNumber();
    String getEnvironment();
    String getEnvironmentFlavor();

}
