package no.ssb.dapla.spark.service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.tracing.TracerBuilder;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.helidon.application.DefaultHelidonApplicationBuilder;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;

import static java.util.Optional.ofNullable;

public class ApplicationBuilder extends DefaultHelidonApplicationBuilder {
    ManagedChannel catalogChannel;
    ManagedChannel datasetAccessChannel;

    @Override
    public <T> HelidonApplicationBuilder override(Class<T> clazz, T instance) {
        super.override(clazz, instance);
        if (ManagedChannel.class.isAssignableFrom(clazz)) {
            catalogChannel = (ManagedChannel) instance;
            datasetAccessChannel = (ManagedChannel) instance;
        }
        return this;
    }

    @Override
    public HelidonApplication build() {
        Config config = ofNullable(this.config).orElseGet(() -> createDefaultConfig());

        if (catalogChannel == null) {
            catalogChannel = ManagedChannelBuilder
                    .forAddress(
                            config.get("catalog-service").get("host").asString().orElseThrow(() ->
                                    new RuntimeException("missing configuration: catalog-service.host")),
                            config.get("catalog-service").get("port").asInt().orElseThrow(() ->
                                    new RuntimeException("missing configuration: catalog-service.port"))
                    )
                    .usePlaintext()
                    .build();
        }

        if (datasetAccessChannel == null) {
            datasetAccessChannel = ManagedChannelBuilder
                    .forAddress(
                            config.get("auth-service").get("host").asString().orElseThrow(() ->
                                    new RuntimeException("missing configuration: auth-service.host")),
                            config.get("auth-service").get("port").asInt().orElseThrow(() ->
                                    new RuntimeException("missing configuration: auth-service.port"))
                    )
                    .usePlaintext()
                    .build();
        }

        TracerBuilder<?> tracerBuilder = TracerBuilder.create(config.get("tracing")).registerGlobal(true);
        Tracer tracer = tracerBuilder.build();

        TracingClientInterceptor tracingInterceptor = TracingClientInterceptor.newBuilder()
                .withTracer(tracer)
                .withStreaming()
                .withVerbosity()
                .withOperationName(new OperationNameConstructor() {
                    @Override
                    public <ReqT, RespT> String constructOperationName(MethodDescriptor<ReqT, RespT> method) {
                        return "Grpc client to " + method.getFullMethodName();
                    }
                })
                .withActiveSpanSource(() -> tracer.scopeManager().activeSpan())
                .withTracedAttributes(TracingClientInterceptor.ClientRequestAttribute.ALL_CALL_OPTIONS, TracingClientInterceptor.ClientRequestAttribute.HEADERS)
                .build();

        CatalogServiceGrpc.CatalogServiceFutureStub catalogService = CatalogServiceGrpc.newFutureStub(tracingInterceptor.intercept(catalogChannel));
        AuthServiceGrpc.AuthServiceFutureStub authService = AuthServiceGrpc.newFutureStub(tracingInterceptor.intercept(datasetAccessChannel));

        return new Application(config, tracer, catalogService, authService);
    }
}
