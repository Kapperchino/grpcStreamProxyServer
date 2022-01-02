package io.helidon.examples.quickstart.eventBus.consumeListener;

import com.google.common.eventbus.Subscribe;
import io.helidon.examples.quickstart.Record;
import io.helidon.examples.quickstart.eventBus.consumeEvents.ConsumerEvent;

import java.util.List;

public class KafkaConsumeListener {

    @Subscribe
    public List<Record<String, String>> consumeTopic(ConsumerEvent event) {

        return null;
    }
}
