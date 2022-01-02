package io.helidon.examples.quickstart.producer;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public interface Producer<K, V> {
    public Future<RecordMetadata> publish(String topic, K key, V value);

    public void close();
}
