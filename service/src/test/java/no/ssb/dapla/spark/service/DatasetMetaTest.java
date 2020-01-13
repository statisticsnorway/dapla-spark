package no.ssb.dapla.spark.service;

import io.grpc.stub.StreamObserver;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceImplBase;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetByNameDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByNameDatasetResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

@GrpcMockRegistryConfig(DatasetMetaTest.DatasetMetaTestGrpcMockRegistry.class)
@ExtendWith(IntegrationTestExtension.class)
class DatasetMetaTest {

    @Inject
    TestClient testClient;

    static class DatasetMetaTestGrpcMockRegistry extends GrpcMockRegistry {
        public DatasetMetaTestGrpcMockRegistry() {
            add(new CatalogServiceImplBase() {
                @Override
                public void getByName(GetByNameDatasetRequest request, StreamObserver<GetByNameDatasetResponse> responseObserver) {
                    GetByNameDatasetResponse response = GetByNameDatasetResponse.newBuilder()
                            .setDataset(Dataset.newBuilder()
                                    .setState(Dataset.DatasetState.OUTPUT)
                                    .setValuation(Dataset.Valuation.INTERNAL)
                                    .setPseudoConfig("pC")
                                    .addLocations("f1")
                                    .setId(DatasetId.newBuilder().setId("123").addName("aName"))
                            ).build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            });
            add(new AuthServiceGrpc.AuthServiceImplBase() {
                @Override
                public void hasAccess(AccessCheckRequest request, StreamObserver<AccessCheckResponse> responseObserver) {
                    responseObserver.onNext(AccessCheckResponse.newBuilder().setAllowed(true).build());
                    responseObserver.onCompleted();
                }
            });
        }
    }

    @Test
    void thatStuffWorks() {
        testClient.get("/sparkplugin?name=a/b/c&operation=READ").expect200Ok();
    }
}
