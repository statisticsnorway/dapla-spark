package no.ssb.dapla.spark.service;

import ch.qos.logback.classic.util.ContextInitializer;
import io.grpc.ManagedChannel;
import io.helidon.config.Config;
import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import io.helidon.grpc.server.GrpcServerConfiguration;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.ServerConfiguration;
import io.helidon.webserver.WebServer;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc.AuthServiceFutureStub;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceFutureStub;
import no.ssb.dapla.spark.service.health.Health;
import no.ssb.dapla.spark.service.health.ReadinessSample;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;

public class Application implements HelidonApplication {

    private static final Logger LOG;

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        LOG = LoggerFactory.getLogger(Application.class);
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        HelidonApplication application = new ApplicationBuilder().build();
        application.start().toCompletableFuture().orTimeout(10, TimeUnit.SECONDS)
                .thenAccept(app -> LOG.info("Webserver running at port: {}, Grpcserver running at port: {}, started in {} ms",
                        app.get(WebServer.class).port(), app.get(GrpcServer.class).port(), System.currentTimeMillis() - startTime))
                .exceptionally(throwable -> {
                    LOG.error("While starting application", throwable);
                    System.exit(1);
                    return null;
                });
    }

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }

    Application(Config config, CatalogServiceFutureStub catalogService, AuthServiceFutureStub authService) {
        put(Config.class, config);

        AtomicReference<ReadinessSample> lastReadySample = new AtomicReference<>(new ReadinessSample(false, System.currentTimeMillis()));

        // initialize health, including a database connectivity wait-loop
        Health health = new Health(() -> get(WebServer.class));

        // catalog grpc service
        put(CatalogServiceFutureStub.class, catalogService);

        // dataset access grpc service
        put(AuthServiceFutureStub.class, authService);

        // services
        SparkPluginService sparkPluginService = new SparkPluginService(catalogService, authService);

        // routing
        Routing routing = Routing.builder()
                .register(ProtobufJsonSupport.create())
                .register(MetricsSupport.create())
                .register(health)
                .register("/dataset-meta", sparkPluginService)
                .build();
        put(Routing.class, routing);

        // web-server
        ServerConfiguration configuration = ServerConfiguration.builder(config.get("webserver")).build();
        WebServer webServer = WebServer.create(configuration, routing);
        put(WebServer.class, webServer);

        // grpc-server
        GrpcServer grpcServer = GrpcServer.create(
                GrpcServerConfiguration.create(config.get("grpcserver")),
                GrpcRouting.builder()
                        .register(sparkPluginService)
                        .build()
        );
        put(GrpcServer.class, grpcServer);
    }

    public CompletionStage<Application> start() {
        return get(WebServer.class).start()
                .thenCombine(get(GrpcServer.class).start(), (webServer, grpcServer) -> this);
    }

    public CompletionStage<Application> stop() {
        return get(WebServer.class).shutdown()
                .thenCombine(get(GrpcServer.class).shutdown(), ((webServer, grpcServer) -> this))
                .thenCombine(CompletableFuture.runAsync(() -> shutdownAndAwaitTermination((ManagedChannel) get(CatalogServiceFutureStub.class).getChannel())), (application, aVoid) -> this)
                .thenCombine(CompletableFuture.runAsync(() -> shutdownAndAwaitTermination((ManagedChannel) get(AuthServiceFutureStub.class).getChannel())), (application, aVoid) -> this)
                ;
    }

    void shutdownAndAwaitTermination(ManagedChannel managedChannel) {
        managedChannel.shutdown();
        try {
            if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                managedChannel.shutdownNow(); // Cancel currently executing tasks
                if (!managedChannel.awaitTermination(5, TimeUnit.SECONDS))
                    LOG.error("ManagedChannel did not terminate");
            }
        } catch (InterruptedException ie) {
            managedChannel.shutdownNow(); // (Re-)Cancel if current thread also interrupted
            Thread.currentThread().interrupt();
        }
    }
}
