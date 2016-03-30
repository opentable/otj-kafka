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
package com.opentable.kafka;

import java.util.Properties;
import java.util.function.Supplier;

import org.junit.rules.ExternalResource;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;

public class KafkaBrokerRule extends ExternalResource
{
    private final Supplier<String> zookeeperConnectString;
    private KafkaServer kafka;
    private int port;

    public KafkaBrokerRule(ZookeeperRule zk)
    {
        this(zk::getConnectString);
    }

    public KafkaBrokerRule(Supplier<String> zookeeperConnectString)
    {
        this.zookeeperConnectString = zookeeperConnectString;
    }

    @Override
    protected void before() throws Throwable
    {
        kafka = new KafkaServer(createConfig(),
                KafkaServer.$lessinit$greater$default$2(), KafkaServer.$lessinit$greater$default$3());
        kafka.startup();
    }

    @Override
    protected void after()
    {
        kafka.shutdown();
    }

    private KafkaConfig createConfig()
    {
        port = TestUtils.RandomPort();
        Properties config = TestUtils.createBrokerConfig(1, zookeeperConnectString.get(),
                TestUtils.createBrokerConfig$default$3(), TestUtils.createBrokerConfig$default$4(),
                TestUtils.createBrokerConfig$default$5(), TestUtils.createBrokerConfig$default$6(),
                TestUtils.createBrokerConfig$default$7(), TestUtils.createBrokerConfig$default$8(),
                TestUtils.createBrokerConfig$default$9(), TestUtils.createBrokerConfig$default$10(),
                TestUtils.createBrokerConfig$default$11(), TestUtils.createBrokerConfig$default$12(),
                TestUtils.createBrokerConfig$default$13(), TestUtils.createBrokerConfig$default$14());
        return new KafkaConfig(config);
    }

    public String getKafkaBrokerConnect()
    {
        return "localhost:" + port;
    }

    public KafkaServer getServer()
    {
        return kafka;
    }
}
