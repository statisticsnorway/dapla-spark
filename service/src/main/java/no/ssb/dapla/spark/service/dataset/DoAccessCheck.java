package no.ssb.dapla.spark.service.dataset;

import com.google.common.util.concurrent.FutureCallback;
import io.helidon.common.http.Http;
import io.helidon.webserver.ServerRequest;
import io.helidon.webserver.ServerResponse;
import io.opentracing.Span;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.helidon.application.Tracing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.ssb.helidon.application.Tracing.logError;

class DoAccessCheck implements FutureCallback<AccessCheckResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(DoAccessCheck.class);

    private Span span;
    private ServerRequest request;
    private ServerResponse response;
    private Dataset dataset;

    DoAccessCheck(Span span, ServerRequest request, ServerResponse response, Dataset dataset) {
        this.span = span;
        this.request = request;
        this.response = response;
        this.dataset = dataset;
    }

    static DoAccessCheck create(Span span, ServerRequest request, ServerResponse response, Dataset dataset) {
        return new DoAccessCheck(span, request, response, dataset);
    }

    @Override
    public void onSuccess(@Nullable AccessCheckResponse result) {
        Tracing.restoreTracingContext(request.tracer(), span);

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
        try {
            Tracing.restoreTracingContext(request.tracer(), span);
            logError(span, t, "error in authService.hasAccess()");
            LOG.error("authService.hasAccess()", t);
            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
        } finally {
            span.finish();
        }
    }
}
