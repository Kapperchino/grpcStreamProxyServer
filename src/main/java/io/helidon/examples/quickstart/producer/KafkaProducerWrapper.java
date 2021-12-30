package io.helidon.examples.quickstart.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.enterprise.inject.Default;
import javax.inject.Named;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Named
@Default
public class KafkaProducerWrapper<K,V> implements Producer<K,V>{
    KafkaProducer<K,V> _producer;
    public KafkaProducerWrapper() {
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
        _producer = new KafkaProducer<K, V>(config);
    }

    @Override
    public Future<RecordMetadata> publish(String topic,K key, V value) {
        final ProducerRecord<K, V> pubRecord = new ProducerRecord<>(topic, key, value);
        return _producer.send(pubRecord, (metadata, e) -> {
            System.out.println(metadata);
            if (e != null) {
                System.out.println("Error while producing message to topic :" + topic  + metadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", metadata.topic(), metadata.partition(), metadata.offset());
                System.out.println(message);
            }
        });
    }

    @Override
    public void close(){
        _producer.close();
    }
}
