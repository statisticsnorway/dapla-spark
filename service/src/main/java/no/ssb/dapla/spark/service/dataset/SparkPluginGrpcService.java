package no.ssb.dapla.spark.service.dataset;

import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import no.ssb.dapla.spark.protobuf.DataSetRequest;
import no.ssb.dapla.spark.protobuf.LoadDataSetResponse;
import no.ssb.dapla.spark.protobuf.SaveDataSetResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.ssb.dapla.spark.service.Tracing.logError;
import static no.ssb.dapla.spark.service.Tracing.spanFromGrpc;

public class SparkPluginGrpcService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkPluginGrpcService.class);

    public SparkPluginGrpcService() {
    }

    @Override
    public void saveDataSet(DataSetRequest request, StreamObserver<SaveDataSetResponse> responseObserver) {
        Span span = spanFromGrpc(request, "loadDataSet");
        try {
            throw new UnsupportedOperationException("TODO not yet implemented"); // TODO
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
        Span span = spanFromGrpc(request, "loadDataSet");
        try {
            throw new UnsupportedOperationException("TODO not yet implemented"); // TODO
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
