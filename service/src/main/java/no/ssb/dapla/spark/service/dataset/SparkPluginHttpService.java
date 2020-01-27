package no.ssb.dapla.spark.service.dataset;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.helidon.common.http.Http;
import io.helidon.webserver.Handler;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.helidon.webserver.Service;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.helidon.application.GrpcAuthorizationBearerCallCredentials;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Arrays.asList;
import static no.ssb.helidon.application.Tracing.logError;
import static no.ssb.helidon.application.Tracing.spanFromHttp;
import static no.ssb.helidon.application.Tracing.traceInputMessage;

public class SparkPluginHttpService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginHttpService.class);

    final CatalogServiceFutureStub catalogService;
    final AuthServiceFutureStub authService;

    public SparkPluginHttpService(CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService) {
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
            traceInputMessage(span, dataset);

            Optional<String> maybeUserId = request.queryParams().first("userId");
            if (maybeUserId.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'userId'");
                return;
            }
            String userId = maybeUserId.get();
            span.setTag("userId", userId);

            ListenableFuture<SaveDatasetResponse> saveFuture = catalogService
                    .withCallCredentials(GrpcAuthorizationBearerCallCredentials.from(request.headers()))
                    .save(SaveDatasetRequest.newBuilder()
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
            span.setTag("userId", userId);

            Optional<String> maybeOperation = request.queryParams().first("operation");
            if (maybeOperation.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'operation'");
                span.finish();
                return;
            }
            Role.Privilege operation = Role.Privilege.valueOf(maybeOperation.get());
            span.setTag("operation", operation.name());

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
                span.setTag("valuation", intendedValuation.name());
                Optional<String> maybeState = request.queryParams().first("state");
                if (maybeState.isEmpty()) {
                    response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'state'");
                    span.finish();
                    return;
                }
                intendedState = Role.DatasetState.valueOf(maybeState.get());
                span.setTag("state", intendedState.name());
            }

            Optional<String> maybeName = request.queryParams().first("name");
            if (maybeName.isEmpty()) {
                response.status(Http.Status.BAD_REQUEST_400).send("Missing required query parameter 'name'");
                span.finish();
                return;
            }
            String name = maybeName.get();
            span.setTag("name", name);

            GrpcAuthorizationBearerCallCredentials authorizationBearer = GrpcAuthorizationBearerCallCredentials.from(request.headers());

            String proposedId = request.queryParams().first("proposedId").orElseGet(() -> {
                span.log("using a random generated UUID as proposedId");
                return UUID.randomUUID().toString();
            });
            span.setTag("proposedId", proposedId);

            MapNameToIdRequest mapNameToIdRequest = MapNameToIdRequest.newBuilder()
                    .setProposedId(proposedId)
                    .addAllName(asList(name.split("/")))
                    .build();

            ListenableFuture<MapNameToIdResponse> idFuture = catalogService
                    .withCallCredentials(authorizationBearer)
                    .mapNameToId(mapNameToIdRequest);

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
}
