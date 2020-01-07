package no.ssb.dapla.spark.service;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import io.helidon.metrics.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.spark.protobuf.*;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;

public class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase implements Service {

    private final Timer helloTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("accessTimer");

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/hello", this::getHello);
    }

    private void getHello(ServerRequest serverRequest, ServerResponse serverResponse) {
        Timer.Context timerContext = helloTimer.time();
        try {
            serverResponse.send("hello");
        } finally {
            timerContext.stop();
        }
    }

    @Override
    public void saveDataSet(DataSetRequest request, StreamObserver<SaveDataSetResponse> responseObserver) {
        System.out.println(request.getName());
        responseObserver.onNext(SaveDataSetResponse.newBuilder()
                .setResult("Some result")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void loadDataSet(DataSetRequest request, StreamObserver<LoadDataSetResponse> responseObserver) {
        System.out.println(request.getName());
        responseObserver.onNext(LoadDataSetResponse.newBuilder()
                .setDataset(DataSet.newBuilder()
                        .setName("konto")
                        .setId("some guid")
                        .setNameSpace("some nameSpace")
                        .build())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
        System.out.println(request.getGreeting());

        try {
            responseObserver.onNext(HelloResponse.newBuilder().setReply("hello").build());
            responseObserver.onCompleted();

        } catch (Exception ex) {
            responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
        }
    }
}