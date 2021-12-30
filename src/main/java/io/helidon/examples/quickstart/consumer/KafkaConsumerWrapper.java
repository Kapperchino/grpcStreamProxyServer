package io.helidon.examples.quickstart.consumer;

import com.google.common.collect.ImmutableList;
import io.helidon.examples.quickstart.Record;
import io.helidon.examples.quickstart.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.enterprise.inject.Default;
import javax.inject.Named;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Named
@Default
public class KafkaConsumerWrapper<K, V> implements Consumer<K, V> {
    KafkaConsumer<K, V> _consumer;

    public KafkaConsumerWrapper() {
        Properties config = new Properties();
        try {
            config.put("client.id", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        config.put("bootstrap.servers", "localhost:9092");
        config.put("acks", "all");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        _consumer = new KafkaConsumer<K, V>(config);
    }

    @Override
    public List<Record<K, V>> consume(String topic) {
        _consumer.subscribe(List.of(topic));
        var builder = ImmutableList.<Record<K,V>>builder();
        try {
            while (true) {
                ConsumerRecords<K, V> records = _consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<K, V> record : records) {
                    builder.add(Record.<K,V>builder().offset(record.offset()).key(record.key()).val(record.value()).build());
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            return builder.build();
        } finally {
            _consumer.close();
        }
    }

    public static void consumeTask(String topic) {

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
}
