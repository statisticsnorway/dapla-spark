package no.ssb.dapla.spark.service;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetResponse;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.dapla.spark.protobuf.DataSet;
import no.ssb.dapla.spark.protobuf.DataSetRequest;
import no.ssb.dapla.spark.protobuf.LoadDataSetResponse;
import no.ssb.dapla.spark.protobuf.SaveDataSetResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

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
        Optional<String> maybeUserId = request.queryParams().first("userId");
        if (maybeUserId.isEmpty()) {
            response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'userId'");
            return;
        }
        String userId = maybeUserId.get();

        Optional<String> maybeOperation = request.queryParams().first("operation");
        if (maybeOperation.isEmpty()) {
            response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'operation'");
            return;
        }
        String operation = maybeOperation.get();

        Optional<String> maybeName = request.queryParams().first("name");
        if (maybeName.isEmpty()) {
            response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'name'");
            return;
        }
        String name = maybeName.get();

        String proposedId = request.queryParams().first("proposedId").orElseGet(() -> UUID.randomUUID().toString());

        MapNameToIdRequest mapNameToIdRequest = MapNameToIdRequest.newBuilder()
                .setProposedId(proposedId)
                .addAllName(asList(name.split("/")))
                .build();

        ListenableFuture<MapNameToIdResponse> idFuture = catalogService.mapNameToId(mapNameToIdRequest);

        Futures.addCallback(idFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable MapNameToIdResponse result) {
                if (ofNullable(result).map(MapNameToIdResponse::getId).orElse("").isBlank()) {
                    response.status(Http.Status.NOT_FOUND_404).send();
                    return;
                }

                ListenableFuture<GetByIdDatasetResponse> datasetFuture = catalogService.getById(GetByIdDatasetRequest.newBuilder()
                        .setId(result.getId())
                        .build()
                );

                Futures.addCallback(datasetFuture, new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable GetByIdDatasetResponse result) {
                        if (ofNullable(result)
                                .map(GetByIdDatasetResponse::getDataset)
                                .map(Dataset::getId)
                                .map(DatasetId::getId)
                                .orElse("")
                                .isBlank()) {
                            response.status(Http.Status.NOT_FOUND_404).send();
                            return;
                        }

                        Dataset dataset = result.getDataset();

                        AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                                .setUserId(userId)
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
                                LOG.error("Failed to do access check", t);
                                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                            }
                        }, MoreExecutors.directExecutor());
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOG.error("Failed to acquire dataset", t);
                        response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    }
                }, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to get dataset id", t);
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
}
