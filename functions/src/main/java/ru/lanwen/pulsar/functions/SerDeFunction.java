package ru.lanwen.pulsar.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class SerDeFunction implements Function<byte[], byte[]> {

    final static TreeNodeSerDe SERDE = new TreeNodeSerDe();

    @Override
    public byte[] process(byte[] input, Context ctx) throws Exception {
        JsonNode value = SERDE.deserialize(input);

        Logger log = ctx.getLogger();
        String v = value.get("value").asText();
        log.info("value found - {}", v);

        ((ObjectNode) value).put("field", "changed");

        return SERDE.serialize(value);
    }

}
