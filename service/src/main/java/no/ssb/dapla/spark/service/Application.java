package no.ssb.dapla.spark.service;

import io.helidon.grpc.server.GrpcRouting;
import io.helidon.grpc.server.GrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.LogManager;

public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

        setupLogging();

        SparkPluginService sparkPluginService = new SparkPluginService();

        GrpcServer grpcServer = GrpcServer
                .create(GrpcRouting.builder()
                        .register(sparkPluginService)
                        .build())
                .start()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        log.info("gRPC Server started at: http://localhost:{}", grpcServer.port());
    }

    /**
     * Disable the JUL hendler and instal the SLF4J bridge.
     */
    private static void setupLogging() {
        // TODO: Find where the ContextInitializer comes from.
        //String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        // if (logbackConfigurationFile != null) {
        //    System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        // }
        LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

}