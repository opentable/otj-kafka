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
package com.opentable.kafka.embedded;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.common.base.Throwables;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class EmbeddedZookeeper implements Watcher, Closeable
{
    private TestingServer server;
    private final BlockingQueue<WatchedEvent> eventQueue = new LinkedBlockingDeque<>();

    public void start()
    {
        try {
            server = new TestingServer();

            try (ZooKeeper zk = new ZooKeeper(getConnectString(), 10000, this)) {
                while (!zk.getState().isConnected()) {
                    eventQueue.take();
                }
            }
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        try {
            server.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getConnectString()
    {
        return server.getConnectString();
    }

    @Override
    public void process(WatchedEvent event) {
        eventQueue.add(event);
    }
}
