package io.helidon.examples.quickstart.mp;

import io.grpc.stub.StreamObserver;
import io.helidon.examples.quickstart.Record;
import io.helidon.examples.quickstart.producer.KafkaProducerWrapper;
import io.helidon.examples.quickstart.producer.Producer;
import io.helidon.grpc.server.CollectingObserver;
import io.helidon.microprofile.grpc.core.ClientStreaming;
import io.helidon.microprofile.grpc.core.Grpc;
import io.helidon.microprofile.grpc.core.GrpcMarshaller;
import io.helidon.microprofile.grpc.core.ServerStreaming;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.ApplicationPath;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@Grpc
@ApplicationScoped
@ApplicationPath("/")
@GrpcMarshaller("jsonb")
public class ConsumeService {
    @ClientStreaming
    public StreamObserver<Record<String,String>> consume(StreamObserver<String> response) {
        return new StreamObserver<>() {
            final List<Future<?>> list = new ArrayList<>();
            final Producer<String, String> producer = new KafkaProducerWrapper<>();

            @Override
            public void onNext(Record<String,String> record) {
                if(record.topic != null && record.key != null && record.val != null){
                    System.out.printf("%s,%s,%s%n",record.topic,record.key,record.val);
                    list.add(producer.publish(record.topic, record.key, record.val));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println(throwable);
            }

            @Override
            public void onCompleted() {
                StringBuilder sb = new StringBuilder();
                for (var future : list) {
                    try {
                        sb.append(future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                response.onNext(
                        sb.toString()
                );
                response.onCompleted();
            }
        };
    }

}
