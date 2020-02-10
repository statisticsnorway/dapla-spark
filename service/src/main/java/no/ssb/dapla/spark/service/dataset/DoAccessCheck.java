package no.ssb.dapla.spark.service.dataset;

import com.google.common.util.concurrent.FutureCallback;
import io.helidon.common.http.Http;
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
        Tracing.tracer().scopeManager().activate(span);

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
        Tracing.tracer().scopeManager().activate(span);

        try {
            logError(span, t, "error in authService.hasAccess()");
            LOG.error("authService.hasAccess()", t);
            response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(t.getMessage());
        } finally {
            span.finish();
        }
    }
}
