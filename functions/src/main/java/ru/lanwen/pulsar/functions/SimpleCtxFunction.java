package ru.lanwen.pulsar.functions;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.nio.ByteBuffer;

public class SimpleCtxFunction implements Function<byte[], String> {

    @Override
    public String process(byte[] input, Context ctx) throws Exception {
        ctx.putState(ctx.getCurrentRecord().getKey().orElse(""), ByteBuffer.wrap(new byte[] {1}));
        ctx.incrCounter(ctx.getCurrentRecord().getKey().orElse("empty"), 1);

        return String.join(
                ":",
                String.valueOf(ctx.getCounter("0")),
                String.valueOf(ctx.getCounter("1")),
                String.valueOf(ctx.getCounter("2")),
                String.valueOf(ctx.getCounter("3")),
                String.valueOf(ctx.getCounter("4"))
        );
    }

}
