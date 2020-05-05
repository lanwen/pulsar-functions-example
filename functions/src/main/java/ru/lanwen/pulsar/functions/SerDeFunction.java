package ru.lanwen.pulsar.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class SerDeFunction implements Function<JsonNode, JsonNode> {

    @Override
    public JsonNode process(JsonNode input, Context ctx) throws Exception {
        Logger log = ctx.getLogger();
        log.info("value found - {}", input.get("value").asText());

        ((ObjectNode) input).put("field", "changed");

        return input;
    }

}
