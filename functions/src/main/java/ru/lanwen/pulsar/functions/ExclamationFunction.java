package ru.lanwen.pulsar.functions;

import java.util.function.Function;

public class ExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String s) {
        return s + "!";
    }
}
