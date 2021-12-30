package io.helidon.examples.quickstart.consumer;

import io.helidon.examples.quickstart.Record;

import java.util.List;

public interface Consumer<K,V>{
    public List<Record<K, V>> consume(String topic);
    public List<Record<K,V>> consume(String topic,int numRecords);
    public List<Record<K,V>> consume(String topic,int numRecords, int offset);
    public void close();
}