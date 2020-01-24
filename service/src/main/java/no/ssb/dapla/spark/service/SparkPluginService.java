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
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.auth.dataset.protobuf.Role;
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
import no.ssb.dapla.spark.service.utils.NamespaceUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static no.ssb.dapla.spark.service.Tracing.logError;
import static no.ssb.dapla.spark.service.Tracing.spanFromGrpc;
import static no.ssb.dapla.spark.service.Tracing.spanFromHttp;

public class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginService.class);

    final CatalogServiceFutureStub catalogService;

    final AuthServiceFutureStub authService;

    static class MapNameToDataset implements FutureCallback<MapNameToIdResponse> {

        private Span span;
        private ServerResponse response;
        private String userId;
        private String name;
        private Role.Privilege operation;
        private Role.Valuation intendedValuation;
        private Role.DatasetState intendedState;
        private CatalogServiceFutureStub catalogService;
        private AuthServiceFutureStub authService;
        private CallCredentials authorizationBearer;

        MapNameToDataset(Span span, ServerResponse response, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            this.span = span;
            this.response = response;
            this.userId = userId;
            this.name = name;
            this.operation = operation;
            this.intendedValuation = intendedValuation;
            this.intendedState = intendedState;
            this.catalogService = catalogService;
            this.authService = authService;
            this.authorizationBearer = authorizationBearer;
        }

        static MapNameToDataset create(Span span, ServerResponse response, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            return new MapNameToDataset(span, response, userId, name, operation, intendedValuation, intendedState, catalogService, authService, authorizationBearer);
        }

        @Override
        public void onSuccess(@Nullable MapNameToIdResponse result) {
            GlobalTracer.get().scopeManager().activate(span);

            if (ofNullable(result).map(MapNameToIdResponse::getId).orElse("").isBlank()) {
                response.status(Http.Status.NOT_FOUND_404).send();
                span.finish();
                return;
            }

            ListenableFuture<GetByIdDatasetResponse> datasetFuture = catalogService.withCallCredentials(authorizationBearer).getById(GetByIdDatasetRequest.newBuilder()
                    .setId(result.getId())
                    .build()
            );

            Futures.addCallback(datasetFuture, GetDataset.create(span, response, result.getId(), userId, name, operation, intendedValuation, intendedState, authService, authorizationBearer), MoreExecutors.directExecutor());
        }

        @Override
        public void onFailure(Throwable t) {
            GlobalTracer.get().scopeManager().activate(span);

            try {
                logError(span, t, "error in catalogService.mapNameToId()");
                LOG.error("catalogService.mapNameToId()", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            } finally {
                span.finish();
            }
        }
    }

    static class GetDataset implements FutureCallback<GetByIdDatasetResponse> {

        private Span span;
        private String mappedId;
        private ServerResponse response;
        private String userId;
        private String name;
        private Role.Privilege operation;
        private Role.Valuation intendedValuation;
        private Role.DatasetState intendedState;
        private AuthServiceFutureStub authService;
        private CallCredentials authorizationBearer;

        GetDataset(Span span, ServerResponse response, String mappedId, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            this.span = span;
            this.mappedId = mappedId;
            this.response = response;
            this.userId = userId;
            this.name = name;
            this.operation = operation;
            this.intendedValuation = intendedValuation;
            this.intendedState = intendedState;
            this.authService = authService;
            this.authorizationBearer = authorizationBearer;
        }

        static GetDataset create(Span span, ServerResponse response, String mappedId, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
            return new GetDataset(span, response, mappedId, userId, name, operation, intendedValuation, intendedState, authService, authorizationBearer);
        }

        @Override
        public void onSuccess(@Nullable GetByIdDatasetResponse result) {
            GlobalTracer.get().scopeManager().activate(span);

            Dataset dataset;
            if (ofNullable(result)
                    .map(GetByIdDatasetResponse::getDataset)
                    .map(Dataset::getId)
                    .map(DatasetId::getId)
                    .orElse("")
                    .isBlank()) {

                if (Set.of(Role.Privilege.READ, Role.Privilege.DELETE).contains(operation)) {
                    response.status(Http.Status.NOT_FOUND_404).send();
                    span.finish();
                    return;
                }

                dataset = Dataset.newBuilder().setId(DatasetId.newBuilder().setId(mappedId).build()).build();
            } else {
                dataset = result.getDataset();
            }

            boolean createOrUpdate = Set.of(Role.Privilege.CREATE, Role.Privilege.UPDATE).contains(operation);

            AccessCheckRequest checkRequest = AccessCheckRequest.newBuilder()
                    .setUserId(userId)
                    .setNamespace(NamespaceUtils.normalize(name))
                    .setPrivilege(operation.name())
                    .setValuation(createOrUpdate ? intendedValuation.name() : dataset.getValuation().name())
                    .setState(createOrUpdate ? intendedState.name() : dataset.getState().name())
                    .build();

            ListenableFuture<AccessCheckResponse> hasAccessListenableFuture = authService.withCallCredentials(authorizationBearer).hasAccess(checkRequest);

            Futures.addCallback(hasAccessListenableFuture, DoAccessCheck.create(span, response, dataset), MoreExecutors.directExecutor());
        }

        @Override
        public void onFailure(Throwable t) {
            GlobalTracer.get().scopeManager().activate(span);

            try {
                logError(span, t, "error in catalogService.getById()");
                LOG.error("catalogService.getById()", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            } finally {
                span.finish();
            }
        }
    }

    static class DoAccessCheck implements FutureCallback<AccessCheckResponse> {

        private Span span;
        private ServerResponse response;
        private Dataset dataset;

        DoAccessCheck(Span span, ServerResponse response, Dataset dataset) {
            this.span = span;
            this.response = response;
            this.dataset = dataset;
        }

        static DoAccessCheck create(Span span, ServerResponse response, Dataset dataset) {
            return new DoAccessCheck(span, response, dataset);
        }

        @Override
        public void onSuccess(@Nullable AccessCheckResponse result) {
            GlobalTracer.get().scopeManager().activate(span);

            if (result != null && result.getAllowed()) {
                response.status(Http.Status.OK_200).send(dataset);
                span.finish();
                return;
            }
            response.status(Http.Status.FORBIDDEN_403).send();
            span.finish();
        }

        @Override
        public void onFailure(Throwable t) {
            GlobalTracer.get().scopeManager().activate(span);

            try {
                logError(span, t, "error in authService.hasAccess()");
                LOG.error("authService.hasAccess()", t);
                response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
            } finally {
                span.finish();
            }
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
        Span span = spanFromHttp(request, "createDatasetMeta");
        try {
            Optional<String> maybeUserId = request.queryParams().first("userId");
            if (maybeUserId.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'userId'");
                return;
            }
            String userId = maybeUserId.get();

            ListenableFuture<SaveDatasetResponse> saveFuture = catalogService.withCallCredentials(AuthorizationBearer.from(request.headers())).save(SaveDatasetRequest.newBuilder()
                    .setDataset(dataset)
                    .setUserId(userId)
                    .build());

            Futures.addCallback(saveFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(@Nullable SaveDatasetResponse result) {
                    response.headers().add("Location", "/dataset-meta");
                    response.status(Http.Status.OK_200).send();
                    span.finish();
                }

                @Override
                public void onFailure(Throwable t) {
                    try {
                        logError(span, t, "error in catalogService.save()");
                        LOG.error("catalogService.save()", t);
                        response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
                    } finally {
                        span.finish();
                    }
                }
            }, MoreExecutors.directExecutor());
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }

    void getDatasetMeta(ServerRequest request, ServerResponse response) {
        Span span = spanFromHttp(request, "getDatasetMeta");
        try {
            Optional<String> maybeUserId = request.queryParams().first("userId");
            if (maybeUserId.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'userId'");
                span.finish();
                return;
            }
            String userId = maybeUserId.get();

            Optional<String> maybeOperation = request.queryParams().first("operation");
            if (maybeOperation.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'operation'");
                span.finish();
                return;
            }
            Role.Privilege operation = Role.Privilege.valueOf(maybeOperation.get());

            Role.Valuation intendedValuation = null;
            Role.DatasetState intendedState = null;
            if (Set.of(Role.Privilege.CREATE, Role.Privilege.UPDATE).contains(operation)) {
                Optional<String> maybeValuation = request.queryParams().first("valuation");
                if (maybeValuation.isEmpty()) {
                    response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'valuation'");
                    span.finish();
                    return;
                }
                intendedValuation = Role.Valuation.valueOf(maybeValuation.get());
                Optional<String> maybeState = request.queryParams().first("state");
                if (maybeState.isEmpty()) {
                    response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'state'");
                    span.finish();
                    return;
                }
                intendedState = Role.DatasetState.valueOf(maybeState.get());
            }

            Optional<String> maybeName = request.queryParams().first("name");
            if (maybeName.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'name'");
                span.finish();
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

            Futures.addCallback(idFuture, MapNameToDataset.create(span, response, userId, name, operation, intendedValuation, intendedState, catalogService, authService, authorizationBearer), MoreExecutors.directExecutor());
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
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
                return s.substring("Bearer ".length());
            }).orElse("no-token");
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
        Span span = spanFromGrpc("loadDataSet");
        try {
            System.out.println(request.getName());
            span.log("TODO: responding with bullshit data"); // TODO
            responseObserver.onNext(SaveDataSetResponse.newBuilder()
                    .setResult("Some result")
                    .build());
            responseObserver.onCompleted();
            span.finish();
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }

    @Override
    public void loadDataSet(DataSetRequest request, StreamObserver<LoadDataSetResponse> responseObserver) {
        Span span = spanFromGrpc("loadDataSet");
        try {
            System.out.println(request.getName());
            span.log("TODO: responding with bullshit data"); // TODO
            responseObserver.onNext(LoadDataSetResponse.newBuilder()
                    .setDataset(DataSet.newBuilder()
                            .setName("konto")
                            .setId("some guid")
                            .setNameSpace("some nameSpace")
                            .build())
                    .build());
            responseObserver.onCompleted();
            span.finish();
        } catch (RuntimeException | Error e) {
            try {
                logError(span, e, "top-level error");
                LOG.error("top-level error", e);
                throw e;
            } finally {
                span.finish();
            }
        }
    }
}
