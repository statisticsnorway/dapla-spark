package no.ssb.dapla.spark.service.dataset;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallCredentials;
import io.helidon.common.http.Http;
import io.helidon.webserver.ServerResponse;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetResponse;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.helidon.application.Tracing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;
import static no.ssb.helidon.application.Tracing.logError;

class MapNameToDataset implements FutureCallback<MapNameToIdResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(MapNameToDataset.class);

    private Span span;
    private ServerResponse response;
    private String userId;
    private String name;
    private Role.Privilege operation;
    private Role.Valuation intendedValuation;
    private Role.DatasetState intendedState;
    private CatalogServiceGrpc.CatalogServiceFutureStub catalogService;
    private AuthServiceGrpc.AuthServiceFutureStub authService;
    private CallCredentials authorizationBearer;

    MapNameToDataset(Span span, ServerResponse response, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, CatalogServiceGrpc.CatalogServiceFutureStub catalogService, AuthServiceGrpc.AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
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

    static MapNameToDataset create(Span span, ServerResponse response, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, CatalogServiceGrpc.CatalogServiceFutureStub catalogService, AuthServiceGrpc.AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
        return new MapNameToDataset(span, response, userId, name, operation, intendedValuation, intendedState, catalogService, authService, authorizationBearer);
    }

    @Override
    public void onSuccess(@Nullable MapNameToIdResponse result) {
        Tracing.tracer().scopeManager().activate(span);

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
        Tracing.tracer().scopeManager().activate(span);

        try {
            logError(span, t, "error in catalogService.mapNameToId()");
            LOG.error("catalogService.mapNameToId()", t);
            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
        } finally {
            span.finish();
        }
    }
}
