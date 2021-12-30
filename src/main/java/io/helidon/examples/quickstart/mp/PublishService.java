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
import javax.inject.Inject;
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
public class PublishService {
    @ClientStreaming
    public StreamObserver<Record<String,String>> publish(StreamObserver<String> response) {
        return new StreamObserver<>() {
            final List<Future<?>> list = new ArrayList<>();
            final Producer<String, String> producer = new KafkaProducerWrapper<>();

            @Override
            public void onNext(Record<String,String> record) {
                if(record.topic != null){
                    list.add(producer.publish(record.topic, record.key, record.val));
                    for (var future : list) {
                        try {
                            System.out.println(future.get());
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                    //producer.close();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println(throwable);
            }

            @Override
            public void onCompleted() {

            }
        };
    }

}
