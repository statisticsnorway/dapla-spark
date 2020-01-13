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

@ApplicationConfig(DatasetMetaTest.DatasetMetaTestRegistry.class)
@ExtendWith(IntegrationTestExtension.class)
class DatasetMetaTest {

    @Inject
    private Application application;

    @Inject
    TestClient testClient;

    static class DatasetMetaTestRegistry extends ApplicationRegistry{

        public DatasetMetaTestRegistry() {
            CatalogServiceImplBase catalogMockImpl = new CatalogServiceImplBase() {
                @Override
                public void getByName(GetByNameDatasetRequest request, StreamObserver<GetByNameDatasetResponse> responseObserver) {
                    GetByNameDatasetResponse response = GetByNameDatasetResponse.newBuilder()
                            .setDataset(
                                    Dataset.newBuilder()
                                            .setState(Dataset.DatasetState.OUTPUT)
                                            .setValuation(Dataset.Valuation.INTERNAL)
                                            .setPseudoConfig("pC")
                                            .addLocations("f1")
                                            .setId(
                                                    DatasetId.newBuilder()
                                                            .setId("123")
                                                            .addName("aName")
                                            )
                            )
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            };

            put(CatalogServiceImplBase.class, catalogMockImpl);

            AuthServiceGrpc.AuthServiceImplBase authServiceImplBase = new AuthServiceGrpc.AuthServiceImplBase() {
                @Override
                public void hasAccess(AccessCheckRequest request, StreamObserver<AccessCheckResponse> responseObserver) {
                    responseObserver.onNext(AccessCheckResponse.newBuilder().setAllowed(true).build());
                    responseObserver.onCompleted();
                }
            };

            put(AuthServiceGrpc.AuthServiceImplBase.class, authServiceImplBase);
        }


    }

    @Test
    void thatStuffWorks() {

        testClient.get("/sparkplugin?name=a/b/c&operation=READ").expect200Ok();
    }
}
