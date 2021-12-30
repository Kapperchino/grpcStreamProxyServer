package io.helidon.examples.quickstart;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Record<K,V> {
    public K key;
    public V val;
    public long offset;
    public String topic;
}
