package com.blitzlines.streams.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Generic JSON Serde for Kafka Streams using Jackson.
 * Handles serialization/deserialization of POJOs to/from JSON bytes.
 * 
 * Supports Kafka Connect envelope format: automatically unwraps {"schema": ..., "payload": ...}
 * 
 * @param <T> The type to serialize/deserialize
 */
public class JsonSerde<T> implements Serde<T> {
    
    private static final Logger log = LoggerFactory.getLogger(JsonSerde.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final Class<T> targetType;
    private final JsonSerializer<T> serializer;
    private final JsonDeserializer<T> deserializer;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
        this.serializer = new JsonSerializer<>();
        this.deserializer = new JsonDeserializer<>(targetType);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    /**
     * JSON Serializer implementation.
     */
    public static class JsonSerializer<T> implements Serializer<T> {
        
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No configuration needed
        }

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return mapper.writeValueAsBytes(data);
            } catch (IOException e) {
                log.error("Error serializing object to JSON for topic {}: {}", topic, e.getMessage());
                throw new RuntimeException("Failed to serialize object", e);
            }
        }

        @Override
        public void close() {
            // No resources to close
        }
    }

    /**
     * JSON Deserializer implementation.
     * Automatically unwraps Kafka Connect envelope format: {"schema": ..., "payload": ...}
     */
    public static class JsonDeserializer<T> implements Deserializer<T> {
        
        private final Class<T> targetType;

        public JsonDeserializer(Class<T> targetType) {
            this.targetType = targetType;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // No configuration needed
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                // First, parse as generic JSON to check for Kafka Connect envelope
                JsonNode root = mapper.readTree(data);
                
                // Check if this is a Kafka Connect envelope with schema + payload
                if (root.has("schema") && root.has("payload")) {
                    // Unwrap the payload from the envelope
                    JsonNode payload = root.get("payload");
                    log.trace("Unwrapping Kafka Connect envelope for topic {}", topic);
                    return mapper.treeToValue(payload, targetType);
                }
                
                // Not an envelope, deserialize directly
                return mapper.treeToValue(root, targetType);
            } catch (IOException e) {
                log.error("Error deserializing JSON from topic {}: {}", topic, e.getMessage());
                log.debug("Raw data: {}", new String(data));
                throw new RuntimeException("Failed to deserialize JSON", e);
            }
        }

        @Override
        public void close() {
            // No resources to close
        }
    }
}
