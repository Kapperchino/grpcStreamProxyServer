package io.helidon.examples.quickstart.eventBus;

import com.google.common.eventbus.EventBus;

public class EventBusFactory {

    //hold the instance of the event bus here
    private static final EventBus eventBus = new EventBus();

    public static EventBus getEventBus() {
        return eventBus;
    }
}