package ru.lanwen.pulsar.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Random;

public class WriteCtxFunction implements Function<byte[], byte[]> {

    final static TreeNodeSerDe SERDE = new TreeNodeSerDe();

    @Override
    public byte[] process(byte[] input, Context ctx) throws Exception {
        JsonNode value = SERDE.deserialize(input);

        Logger log = ctx.getLogger();
        String v = value.get("value").asText();
        log.info("value found - {}", v);

        String key = String.valueOf(new Random().nextInt() % 5);

        ctx.putState(key, ByteBuffer.wrap(new byte[] {1}));
        ctx.incrCounter(key, 1L);

        ((ObjectNode) value).put("field", "changed");

        return SERDE.serialize(value);
    }

}
