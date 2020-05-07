package ru.lanwen.pulsar.functions;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

public class Consumer {

    static Flux<Message<byte[]>> consumer(PulsarClient client, String testInputsTopic) {
        return Flux
                .usingWhen(
                        Mono.fromCompletionStage(() -> client.newConsumer()
                                .subscriptionType(SubscriptionType.Exclusive)
                                .consumerName("consumer-" + UUID.randomUUID())
                                .subscriptionName("subscription-" + UUID.randomUUID())
                                .topic(testInputsTopic)
                                .subscribeAsync()
                        ),
                        consumer -> Mono.fromCompletionStage(consumer::receiveAsync)
                                .delayUntil(msg -> Mono.fromCompletionStage(consumer.acknowledgeAsync(msg)))
                                .repeat(),
                        consumer -> Mono.fromCompletionStage(consumer.closeAsync())
                )
                .doOnNext(msg -> {
                    System.out.printf("[%s]: %s (key:%s)%n", testInputsTopic, new String(msg.getData()), msg.getKey());
                });
    }
}
