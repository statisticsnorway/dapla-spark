package no.ssb.dapla.spark.service;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.helidon.common.http.Http;
import io.helidon.metrics.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceStub;
import no.ssb.dapla.catalog.protobuf.GetDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetDatasetResponse;
import no.ssb.dapla.spark.protobuf.DataSet;
import no.ssb.dapla.spark.protobuf.DataSetRequest;
import no.ssb.dapla.spark.protobuf.HelloRequest;
import no.ssb.dapla.spark.protobuf.HelloResponse;
import no.ssb.dapla.spark.protobuf.LoadDataSetResponse;
import no.ssb.dapla.spark.protobuf.SaveDataSetResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginService.class);

    private final Timer helloTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("accessTimer");

    final CatalogServiceStub catalogService;

    public SparkPluginService(CatalogServiceStub catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/prepareRead", this::prepareRead);
    }

    void prepareRead(ServerRequest request, ServerResponse response) {

        GetDatasetRequest datasetRequest = GetDatasetRequest.newBuilder()
                .setId("123")
                .build();


        StreamObserver<GetDatasetResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(GetDatasetResponse value) {
                LOG.info("Found dataset {}", value.getDataset().toString());
                response.status(Http.Status.OK_200).send(value.getDataset());
            }

            @Override
            public void onError(Throwable t) {
                LOG.error("Could not get dataset", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            }

            @Override
            public void onCompleted() {
                LOG.info("Done");
            }
        };

        catalogService.get(datasetRequest, responseObserver);
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