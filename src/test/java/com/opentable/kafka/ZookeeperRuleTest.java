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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Rule;
import org.junit.Test;

public class ZookeeperRuleTest implements Watcher {
    @Rule
    public ZookeeperRule zk = new ZookeeperRule();

    private final List<WatchedEvent> events = new ArrayList<>();

    @Test(timeout = 60000)
    public void testBoots() throws Exception {
        final String connect = zk.getConnectString();
        ZooKeeper client = new ZooKeeper(connect, 60000, this);

        while (!client.getState().isConnected()) {
            synchronized (this) {
                wait();
            }
        }

        assertFalse(events.isEmpty());
        assertEquals(KeeperState.SyncConnected, events.get(0).getState());

        client.close();
    }

    @Override
    public void process(WatchedEvent event) {
        events.add(event);
        synchronized (this) {
            notifyAll();
        }
    }
}
