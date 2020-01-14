package no.ssb.dapla.spark.service;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.helidon.config.Config;
import io.helidon.config.spi.ConfigSource;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc;
import no.ssb.helidon.application.HelidonApplication;
import no.ssb.helidon.application.HelidonApplicationBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static io.helidon.config.ConfigSources.classpath;
import static io.helidon.config.ConfigSources.file;

public class ApplicationBuilder implements HelidonApplicationBuilder {
    Config config;
    ManagedChannel globalGrpcClientChannel;

    @Override
    public <T> HelidonApplicationBuilder override(Class<T> clazz, T instance) {
        if (Config.class.isAssignableFrom(clazz)) {
            config = (Config) instance;
        }
        if (ManagedChannel.class.isAssignableFrom(clazz)) {
            globalGrpcClientChannel = (ManagedChannel) instance;
        }
        return this;
    }

    @Override
    public HelidonApplication build() {
        if (config == null) {
            List<Supplier<ConfigSource>> configSourceSupplierList = new LinkedList<>();
            String overrideFile = System.getenv("HELIDON_CONFIG_FILE");
            if (overrideFile != null) {
                configSourceSupplierList.add(file(overrideFile).optional());
            }
            configSourceSupplierList.add(file("conf/application.yaml").optional());
            configSourceSupplierList.add(classpath("application.yaml"));

            config = Config.builder().sources(configSourceSupplierList).build();
        }

        CatalogServiceGrpc.CatalogServiceFutureStub catalogService;
        AuthServiceGrpc.AuthServiceFutureStub authService;
        if (globalGrpcClientChannel == null) {
            ManagedChannel catalogChannel = ManagedChannelBuilder
                    .forAddress(
                            config.get("catalog-service").get("host").asString().orElse("localhost"),
                            config.get("catalog-service").get("port").asInt().orElse(1408)
                    )
                    .usePlaintext()
                    .build();
            catalogService = CatalogServiceGrpc.newFutureStub(catalogChannel);

            ManagedChannel datasetAccessChannel = ManagedChannelBuilder
                    .forAddress(
                            config.get("auth-service").get("host").asString().orElse("localhost"),
                            config.get("auth-service").get("port").asInt().orElse(7070)
                    )

                    .usePlaintext()
                    .build();
            authService = AuthServiceGrpc.newFutureStub(datasetAccessChannel);
        } else {
            catalogService = CatalogServiceGrpc.newFutureStub(globalGrpcClientChannel);
            authService = AuthServiceGrpc.newFutureStub(globalGrpcClientChannel);
        }

        Application application = new Application(config, catalogService, authService);
        return application;
    }
}
