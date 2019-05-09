/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
