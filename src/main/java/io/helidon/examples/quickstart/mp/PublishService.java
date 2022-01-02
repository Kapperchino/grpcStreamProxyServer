package io.helidon.examples.quickstart.mp;

import io.grpc.stub.StreamObserver;
import io.helidon.examples.quickstart.Record;
import io.helidon.examples.quickstart.consumer.KafkaConsumerWrapper;
import io.helidon.examples.quickstart.producer.KafkaProducerWrapper;
import io.helidon.examples.quickstart.producer.Producer;
import io.helidon.microprofile.grpc.core.ClientStreaming;
import io.helidon.microprofile.grpc.core.Grpc;
import io.helidon.microprofile.grpc.core.GrpcMarshaller;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.ApplicationPath;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@Grpc
@ApplicationScoped
@ApplicationPath("/")
@GrpcMarshaller("jsonb")
@Slf4j
public class PublishService {
    @ClientStreaming
    public StreamObserver<Record<String, String>> publish(StreamObserver<String> response) {
        return new StreamObserver<>() {
            final List<Future<?>> list = new ArrayList<>();
            final Producer<String, String> producer = new KafkaProducerWrapper<>();

            @Override
            public void onNext(Record<String, String> record) {
                list.add(producer.publish(record.getTopic(), record.getKey(), record.getVal()));
                for (var future : list) {
                    try {
                        System.out.println(future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println(throwable);
            }

            @SneakyThrows
            @Override
            public void onCompleted() {
                producer.close();
            }
        };
    }

}
