package no.ssb.dapla.spark.service;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import io.helidon.common.http.Http;
import io.helidon.webserver.Handler;
import io.helidon.webserver.RequestHeaders;
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
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.dapla.spark.protobuf.DataSet;
import no.ssb.dapla.spark.protobuf.DataSetRequest;
import no.ssb.dapla.spark.protobuf.LoadDataSetResponse;
import no.ssb.dapla.spark.protobuf.SaveDataSetResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;

import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;

public class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginService.class);

    final CatalogServiceFutureStub catalogService;

    final AuthServiceFutureStub authService;

    static class MapNameToDataset implements FutureCallback<MapNameToIdResponse> {

        private ServerResponse response;
        private String userId;
        private String name;
        private String operation;
        private CatalogServiceFutureStub catalogService;
        private AuthServiceFutureStub authService;
        private CallCredentials authorizationBearer;

        MapNameToDataset(ServerResponse response, String userId, String name, String operation, CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            this.response = response;
            this.userId = userId;
            this.name = name;
            this.operation = operation;
            this.catalogService = catalogService;
            this.authService = authService;
            this.authorizationBearer = authorizationBearer;
        }

        static MapNameToDataset create(ServerResponse response, String userId, String name, String operation, CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            return new MapNameToDataset(response, userId, name, operation, catalogService, authService, authorizationBearer);
        }

        @Override
        public void onSuccess(@Nullable MapNameToIdResponse result) {
            if (ofNullable(result).map(MapNameToIdResponse::getId).orElse("").isBlank()) {
                response.status(Http.Status.NOT_FOUND_404).send();
                return;
            }

            ListenableFuture<GetByIdDatasetResponse> datasetFuture = catalogService.withCallCredentials(authorizationBearer).getById(GetByIdDatasetRequest.newBuilder()
                    .setId(result.getId())
                    .build()
            );

            Futures.addCallback(datasetFuture, GetDataset.create(response, userId, name, operation, authService, authorizationBearer), MoreExecutors.directExecutor());
        }

        @Override
        public void onFailure(Throwable t) {
            LOG.error("Failed to get dataset id", t);
            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
        }
    }

    static class GetDataset implements FutureCallback<GetByIdDatasetResponse> {

        private ServerResponse response;
        private String userId;
        private String name;
        private String operation;
        private AuthServiceFutureStub authService;
        private CallCredentials authorizationBearer;

        GetDataset(ServerResponse response, String userId, String name, String operation, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            this.response = response;
            this.userId = userId;
            this.name = name;
            this.operation = operation;
            this.authService = authService;
            this.authorizationBearer = authorizationBearer;
        }

        static GetDataset create(ServerResponse response, String userId, String name, String operation, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            return new GetDataset(response, userId, name, operation, authService, authorizationBearer);
        }

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

            ListenableFuture<AccessCheckResponse> hasAccessListenableFuture = authService.withCallCredentials(authorizationBearer).hasAccess(checkRequest);

            Futures.addCallback(hasAccessListenableFuture, DoAccessCheck.create(response, dataset), MoreExecutors.directExecutor());
        }

        @Override
        public void onFailure(Throwable t) {
            LOG.error("Failed to acquire dataset", t);
            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
        }
    }

    static class DoAccessCheck implements FutureCallback<AccessCheckResponse> {

        private ServerResponse response;
        private Dataset dataset;

        DoAccessCheck(ServerResponse response, Dataset dataset) {
            this.response = response;
            this.dataset = dataset;
        }

        static DoAccessCheck create(ServerResponse response, Dataset dataset) {
            return new DoAccessCheck(response, dataset);
        }

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
    }

    public SparkPluginService(CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService) {
        this.catalogService = catalogService;
        this.authService = authService;
    }

    @Override
    public void update(Routing.Rules rules) {
        rules.get("/", this::getDatasetMeta);
        rules.put("/", Handler.create(Dataset.class, this::createDatasetMeta));
    }

    void createDatasetMeta(ServerRequest request, ServerResponse response, Dataset dataset) {
        ListenableFuture<SaveDatasetResponse> saveFuture = catalogService.withCallCredentials(AuthorizationBearer.from(request.headers())).save(SaveDatasetRequest.newBuilder()
                .setDataset(dataset)
                .build());

        Futures.addCallback(saveFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable SaveDatasetResponse result) {
                response.headers().add("Location", "/dataset-meta");
                response.status(Http.Status.OK_200).send();
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error("Failed to create dataset meta", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            }
        }, MoreExecutors.directExecutor());
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

        AuthorizationBearer authorizationBearer = AuthorizationBearer.from(request.headers());

        String proposedId = request.queryParams().first("proposedId").orElseGet(() -> UUID.randomUUID().toString());

        MapNameToIdRequest mapNameToIdRequest = MapNameToIdRequest.newBuilder()
                .setProposedId(proposedId)
                .addAllName(asList(name.split("/")))
                .build();

        ListenableFuture<MapNameToIdResponse> idFuture = catalogService.withCallCredentials(authorizationBearer).mapNameToId(mapNameToIdRequest);

        Futures.addCallback(idFuture, MapNameToDataset.create(response, userId, name, operation, catalogService, authService, authorizationBearer), MoreExecutors.directExecutor());
    }

    static class AuthorizationBearer extends CallCredentials {

        private String token;

        AuthorizationBearer(String token) {
            this.token = token;
        }

        static AuthorizationBearer from(RequestHeaders headers) {
            String token = headers.first("Authorization").map(s -> {
                if (Strings.isNullOrEmpty(s) || !s.startsWith("Bearer ")) {
                    return "";
                }
                return s.split(" ")[1];
            }).orElse("");
            return new AuthorizationBearer(token);
        }

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), String.format("Bearer %s", token));
            appExecutor.execute(() -> applier.apply(metadata));
        }

        @Override
        public void thisUsesUnstableApi() {
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
}
