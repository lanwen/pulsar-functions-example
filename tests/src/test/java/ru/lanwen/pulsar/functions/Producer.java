package ru.lanwen.pulsar.functions;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

public class Producer {

    static Flux<MessageId> json(PulsarClient client, String testInputsTopic) {
        return Flux
                .usingWhen(
                        Mono.fromCompletionStage(() -> client.newProducer(Schema.JSON(Map.class)).topic(testInputsTopic).createAsync()),
                        producer -> Flux.interval(Duration.ofSeconds(2))
                                .map(i -> producer
                                        .newMessage()
                                        .value(Map.of("value", i + "-" + UUID.randomUUID()))
                                        .sendAsync()
                                )
                                .flatMap(Mono::fromCompletionStage),
                        producer -> Mono.fromCompletionStage(producer.closeAsync())
                );
    }

    static Flux<MessageId> string(PulsarClient client, String testInputsTopic) {
        return Flux
                .usingWhen(
                        Mono.fromCompletionStage(() -> client.newProducer(Schema.STRING).topic(testInputsTopic).createAsync()),
                        producer -> Flux.interval(Duration.ofSeconds(5))
                                .map(i -> producer
                                        .newMessage()
                                        .value(i + "-" + UUID.randomUUID())
                                        .sendAsync()
                                )
                                .flatMap(Mono::fromCompletionStage),
                        producer -> Mono.fromCompletionStage(producer.closeAsync())
                );
    }
}
