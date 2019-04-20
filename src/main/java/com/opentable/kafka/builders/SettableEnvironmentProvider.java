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
