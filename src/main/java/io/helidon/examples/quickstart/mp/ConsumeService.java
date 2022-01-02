package io.helidon.examples.quickstart.mp;

import io.grpc.stub.StreamObserver;
import io.helidon.examples.quickstart.Record;
import io.helidon.examples.quickstart.consumer.Consumer;
import io.helidon.examples.quickstart.consumer.KafkaConsumerWrapper;
import io.helidon.microprofile.grpc.core.Grpc;
import io.helidon.microprofile.grpc.core.GrpcMarshaller;
import io.helidon.microprofile.grpc.core.ServerStreaming;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.ApplicationPath;
import java.util.concurrent.ExecutionException;


@Grpc
@ApplicationScoped
@ApplicationPath("/")
@GrpcMarshaller("jsonb")
@Slf4j
public class ConsumeService {
    @ServerStreaming
    public void consume(String topic, StreamObserver<Record<String, String>> response) {
        log.info("Comsume call starts");
        final Consumer<String, String> consumer = new KafkaConsumerWrapper<>();
        var future = consumer.consume(topic);
        try {
            for (var record : future.get()) {
                response.onNext(record);
            }
        } catch (InterruptedException | ExecutionException e) {
            response.onError(e);
            e.printStackTrace();
        } finally {
            consumer.close();
            response.onCompleted();
        }
    }
}
