package ru.lanwen.pulsar.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.SerDe;

import java.io.IOException;

public class TreeNodeSerDe implements SerDe<JsonNode> {

    static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(byte[] input) {
        try {
            return mapper.readTree(input);
        } catch (IOException e) {
            throw new SerDeException("deserialize", e);
        }
    }

    @Override
    public byte[] serialize(JsonNode input) {
        try {
            return mapper.writeValueAsBytes(input);
        } catch (IOException e) {
            throw new SerDeException("serialize", e);
        }
    }

    static class SerDeException extends RuntimeException {
        public SerDeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
