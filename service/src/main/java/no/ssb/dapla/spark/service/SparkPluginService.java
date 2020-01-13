package no.ssb.dapla.spark.service;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.helidon.common.http.Http;
import io.helidon.metrics.RegistryFactory;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetByNameDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByNameDatasetResponse;
import no.ssb.dapla.spark.protobuf.DataSet;
import no.ssb.dapla.spark.protobuf.DataSetRequest;
import no.ssb.dapla.spark.protobuf.HelloRequest;
import no.ssb.dapla.spark.protobuf.HelloResponse;
import no.ssb.dapla.spark.protobuf.LoadDataSetResponse;
import no.ssb.dapla.spark.protobuf.SaveDataSetResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;

public class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginService.class);

    private final Timer helloTimer = RegistryFactory.getInstance().getRegistry(MetricRegistry.Type.APPLICATION).timer("accessTimer");

    final CatalogServiceFutureStub catalogService;

    final AuthServiceFutureStub authService;

    public SparkPluginService(CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService) {
        this.catalogService = catalogService;
        this.authService = authService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/", this::getDatasetMeta);
    }

    void getDatasetMeta(ServerRequest request, ServerResponse response) {

        Optional<String> maybeOperation = request.queryParams().first("operation");
        if (maybeOperation.isEmpty()) {
            response.status(Http.Status.BAD_REQUEST_400).send("Expected 'operation'");
            return;
        }
        String operation = maybeOperation.get();

        Optional<String> maybeName = request.queryParams().first("name");
        if (maybeName.isEmpty()) {
            response.status(Http.Status.BAD_REQUEST_400).send("Expected 'name'");
            return;
        }
        String name = maybeName.get();

        GetByNameDatasetRequest getDatasetRequest = GetByNameDatasetRequest.newBuilder()
                .addAllName(asList(name.split("/")))
                .build();

        ListenableFuture<GetByNameDatasetResponse> datasetFuture = catalogService.getByName(getDatasetRequest);

        Futures.addCallback(datasetFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable GetByNameDatasetResponse datasetResponse) {
                if (ofNullable(datasetResponse)
                        .map(GetByNameDatasetResponse::getDataset)
                        .map(Dataset::getId)
                        .map(DatasetId::getId)
                        .orElse("")
                        .isBlank()) {
                    response.status(Http.Status.NOT_FOUND_404).send();
                    return;
                }

                Dataset dataset = datasetResponse.getDataset();

                AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                        .setUserId("123") // TODO: Extract this from incoming request
                        .setNamespace(name)
                        .setPrivilege(operation)
                        .setValuation(dataset.getValuation().name())
                        .setState(dataset.getState().name())
                        .build();

                ListenableFuture<AccessCheckResponse> hasAccessListenableFuture = authService.hasAccess(checkRequest);

                Futures.addCallback(hasAccessListenableFuture, new FutureCallback<>() {

                    @Override
                    public void onSuccess(@Nullable AccessCheckResponse result) {
                        if (result != null && result.getAllowed()) {
                            response.status(Http.Status.OK_200).send(dataset);
                            return;
                        }
                        response.status(Http.Status.FORBIDDEN_403).send();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOG.error("Failed to get dataset meta", t);
                        response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    }
                }, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to get dataset meta", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            }
        }, MoreExecutors.directExecutor());
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