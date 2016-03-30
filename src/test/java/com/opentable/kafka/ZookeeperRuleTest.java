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
