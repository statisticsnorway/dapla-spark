package no.ssb.dapla.spark.service;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import no.ssb.dapla.spark.protobuf.*;

public class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase {

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