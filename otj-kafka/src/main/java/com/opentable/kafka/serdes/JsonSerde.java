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
package com.opentable.kafka.serdes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializes arbitrary Java data types to and from JSON using Jackson.
 *
 * <p>
 * Implements interfaces convenient for Kafka use.
 *
 * @param <T> the type to serialize and deserialize
 */
public class JsonSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {
    private final ObjectMapper mapper;
    private final JavaType type;

    JsonSerde(ObjectMapper mapper, JavaType type) {
        this.mapper = mapper;
        this.type = type;
    }

    public static <T> JsonSerde<T> forType(ObjectMapper mapper, Class<T> klass) {
        return new JsonSerde<>(mapper, mapper.getTypeFactory().constructType(klass));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return data == null ? null : mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return data == null ? null : mapper.readValue(data, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
