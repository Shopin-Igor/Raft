package ru.hse.drip.systems.raft.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonCodec() {
    }

    public static String encode(WireMessage message) {
        try {
            return MAPPER.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot encode message", e);
        }
    }

    public static WireMessage decode(String line) {
        try {
            return MAPPER.readValue(line, WireMessage.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot decode message: " + line, e);
        }
    }
}
