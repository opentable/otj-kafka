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
package com.opentable.logging;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.mrbean.MrBeanModule;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.opentable.kafka.embedded.EmbeddedKafkaBuilder;
import com.opentable.kafka.embedded.EmbeddedKafkaRule;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.status.OnConsoleStatusListener;

public class KafkaAppenderTest
{
    @Rule
    public final EmbeddedKafkaRule kafka = new EmbeddedKafkaBuilder().withTopics("logs").rule();

    private final LoggerContext context = new LoggerContext();
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new MrBeanModule());

    @SuppressWarnings("UnstableApiUsage")
    @Before
    public void addHandler() throws Exception
    {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        final String xml = Resources.toString(KafkaAppenderTest.class.getResource("/logback-kafka.xml"), Charsets.UTF_8)
                .replaceAll("\\$KAFKA\\$", kafka.getBroker().getKafkaBrokerConnect());

        final OnConsoleStatusListener listener = new OnConsoleStatusListener();
        listener.start();
        context.getStatusManager().add(listener);

        final JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(new ByteArrayInputStream(xml.getBytes(Charsets.UTF_8)));
        context.start();
    }

    @After
    public void removeHandler() throws Exception
    {
        context.stop();
    }

    @Test(timeout=30000)
    public void testLog() throws Exception
    {
        context.getLogger("test").info("Herro!");
        context.getLogger("womp").warn("flop", new Throwable());

        // Ensure we flush our buffers
        context.stop();

        final CommonLogFields log1, log2;

        try (KafkaConsumer<String, String> consumer = kafka.getBroker().createConsumer("test")) {
            consumer.subscribe(Collections.singletonList("logs"));
            final Iterator<ConsumerRecord<String, String>> iterator = consumer.poll(Duration.ofSeconds(10)).iterator();

            log1 = read(iterator.next().value());
            log2 = read(iterator.next().value());
        }

        assertEquals("Herro!", log1.getMessage());
        assertEquals("test", log1.getLogClass());
        assertEquals("INFO", log1.getSeverity());

        assertEquals("flop", log2.getMessage());
        assertEquals("womp", log2.getLogClass());
        assertEquals("WARN", log2.getSeverity());
    }

    private CommonLogFields read(String data) throws IOException
    {
        return mapper.readValue(data, CommonLogFields.class);
    }
}
