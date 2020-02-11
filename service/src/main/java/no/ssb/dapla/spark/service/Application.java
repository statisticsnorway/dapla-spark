package no.ssb.dapla.spark.service;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.helidon.config.Config;
import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import io.helidon.grpc.server.GrpcServerConfiguration;
import io.helidon.grpc.server.GrpcTracingConfig;
import io.helidon.grpc.server.ServerRequestAttribute;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.accesslog.AccessLogSupport;
import io.opentracing.Tracer;
import io.opentracing.contrib.grpc.OperationNameConstructor;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceFutureStub;
import no.ssb.dapla.spark.service.dataset.SparkPluginGrpcService;
import no.ssb.dapla.spark.service.dataset.SparkPluginHttpService;
import no.ssb.dapla.spark.service.health.Health;
import no.ssb.dapla.spark.service.health.ReadinessSample;
import no.ssb.helidon.application.AuthorizationInterceptor;
import no.ssb.helidon.application.DefaultHelidonApplication;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Application extends DefaultHelidonApplication {

    private static final Logger LOG;

    static {
        installSlf4jJulBridge();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        new ApplicationBuilder().build()
                .start()
                .toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    Application(Config config, Tracer tracer, CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService) {
        put(Config.class, config);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        // initialize health, including a database connectivity wait-loop
        Health health = new Health(() -> get(WebServer.class));

        // catalog grpc service
        put(CatalogServiceFutureStub.class, catalogService);

        // dataset access grpc service
        put(AuthServiceFutureStub.class, authService);

        // grpc-server
        SparkPluginGrpcService sparkPluginGrpcService = new SparkPluginGrpcService();
        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.builder(config.get("grpcserver"))
                        .tracer(tracer)
                        .tracingConfig(GrpcTracingConfig.builder()
                                .withStreaming()
                                .withVerbosity()
                                .withOperationName(new OperationNameConstructor() {
                                    @Override
                                    public <ReqT, RespT> String constructOperationName(MethodDescriptor<ReqT, RespT> method) {
                                        return "Grpc server received " + method.getFullMethodName();
                                    }
                                })
                                .withTracedAttributes(ServerRequestAttribute.CALL_ATTRIBUTES,
                                        ServerRequestAttribute.HEADERS,
                                        ServerRequestAttribute.METHOD_NAME)
                                .build()
                        ),
                GrpcRouting.builder()
                        .intercept(new AuthorizationInterceptor())
                        .register(sparkPluginGrpcService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);

        // routing
        Routing routing = Routing.builder()
                .register(AccessLogSupport.create(config.get("webserver.access-log")))
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register(health)
                .register("/dataset-meta", new SparkPluginHttpService(catalogService, authService))
                .build();
        put(Routing.class, routing);

        // web-server
        WebServer webServer = WebServer.create(
                ServerConfiguration.builder(config.get("webserver"))
                        .tracer(tracer)
                        .build(),
                routing);
        put(WebServer.class, webServer);
    }

    @Override
    public CompletionStage<HelidonApplication> stop() {
        return super.stop()
                .thenCombine(CompletableFuture.runAsync(() -> shutdownAndAwaitTermination((ManagedChannel) get(CatalogServiceFutureStub.class).getChannel())), (application, aVoid) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> shutdownAndAwaitTermination((ManagedChannel) get(AuthServiceFutureStub.class).getChannel())), (application, aVoid) -> this)
                ;
    }
}
