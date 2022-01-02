package io.helidon.examples.quickstart.consumer;

import io.helidon.examples.quickstart.Record;
import io.helidon.examples.quickstart.SchedulerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.enterprise.inject.Default;
import javax.inject.Named;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

@Named
@Default
@Slf4j
public class KafkaConsumerWrapper<K, V> implements Consumer<K, V> {
    public final static int MAX_EMPTY_RECORDS = 20;
    KafkaConsumer<K, V> _consumer;

    public KafkaConsumerWrapper() {
        Properties config = new Properties();
        config.put("client.id", UUID.randomUUID().toString());
        config.put("bootstrap.servers", "localhost:9092");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        _consumer = new KafkaConsumer<>(config);
    }

    @Override
    public Future<List<Record<K, V>>> consume(String topic) {
        return SchedulerWrapper.getScheduler().submit(new ConsumeTask(topic));
    }

    @Override
    public List<Record<K, V>> consume(String topic, int offset) {
        return null;
    }

    @Override
    public List<Record<K, V>> consume(String topic, int numRecords, int offset) {
        return null;
    }

    @Override
    public void close() {
        _consumer.close();
    }

    private class ConsumeTask implements Callable<List<Record<K, V>>> {
        String _topic;

        public ConsumeTask(String topic) {
            this._topic = topic;
        }

        @Override
        public List<Record<K, V>> call() {
            _consumer.subscribe(List.of(_topic));
            log.debug("start to consume");
            List<Record<K, V>> result = new ArrayList<>();
            int emptyCount = 0;
            while (emptyCount < MAX_EMPTY_RECORDS) {
                try {
                    ConsumerRecords<K, V> records = _consumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        ++emptyCount;
                    }
                    for (ConsumerRecord<K, V> record : records) {
                        result.add(Record.<K, V>builder().offset(record.offset()).key(record.key()).val(record.value()).topic(record.topic()).build());
                        log.debug("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                    }
                    _consumer.commitAsync();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(e.getMessage());
                }
            }
            return result;
        }
    }
}
