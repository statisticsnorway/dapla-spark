package no.ssb.dapla.spark.service.dataset;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallCredentials;
import io.helidon.common.http.Http;
import io.helidon.webserver.ServerResponse;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.auth.dataset.protobuf.Role;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetResponse;
import no.ssb.dapla.spark.service.utils.NamespaceUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.Optional.ofNullable;
import static no.ssb.helidon.application.Tracing.logError;

class GetDataset implements FutureCallback<GetByIdDatasetResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(GetDataset.class);

    private Span span;
    private String mappedId;
    private ServerResponse response;
    private String userId;
    private String name;
    private Role.Privilege operation;
    private Role.Valuation intendedValuation;
    private Role.DatasetState intendedState;
    private AuthServiceGrpc.AuthServiceFutureStub authService;
    private CallCredentials authorizationBearer;

    GetDataset(Span span, ServerResponse response, String mappedId, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, AuthServiceGrpc.AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
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

    static GetDataset create(Span span, ServerResponse response, String mappedId, String userId, String name, Role.Privilege operation, Role.Valuation intendedValuation, Role.DatasetState intendedState, AuthServiceGrpc.AuthServiceFutureStub authService, CallCredentials authorizationBearer) {
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
