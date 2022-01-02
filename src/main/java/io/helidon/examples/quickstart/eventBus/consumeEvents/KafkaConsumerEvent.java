package io.helidon.examples.quickstart.eventBus.consumeEvents;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
@AllArgsConstructor
public class KafkaConsumerEvent implements ConsumerEvent {
    @NonNull
    String topic;
}
