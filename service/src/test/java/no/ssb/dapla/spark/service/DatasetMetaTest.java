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
import no.ssb.testing.helidon.GrpcMockRegistry;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.TestClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GrpcMockRegistryConfig(DatasetMetaTest.DatasetMetaTestGrpcMockRegistry.class)
@ExtendWith(IntegrationTestExtension.class)
public class DatasetMetaTest {

    @Inject
    TestClient testClient;

    private static final Map<String, Dataset> CATALOG = new HashMap<>();

    static {
        CATALOG.put(
                "a-dataset",
                Dataset.newBuilder()
                        .setState(Dataset.DatasetState.OUTPUT)
                        .setValuation(Dataset.Valuation.OPEN)
                        .setPseudoConfig("pC")
                        .addLocations("f1")
                        .setId(
                                DatasetId.newBuilder()
                                        .setId("123")
                                        .addName("a-dataset")
                        )
                        .build()
        );
    }

    private static final Set<String> ACCESS = Set.of("123");

    public static class DatasetMetaTestGrpcMockRegistry extends GrpcMockRegistry {
        public DatasetMetaTestGrpcMockRegistry() {
            add(new CatalogServiceImplBase() {
                @Override
                public void getByName(GetByNameDatasetRequest request, StreamObserver<GetByNameDatasetResponse> responseObserver) {
                    GetByNameDatasetResponse.Builder responseBuilder = GetByNameDatasetResponse.newBuilder();

                    Dataset dataset = CATALOG.get(request.getNameList().asByteStringList().get(0).toStringUtf8());
                    if (dataset != null) {
                        responseBuilder.setDataset(dataset);
                    }

                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }
            });
            add(new AuthServiceGrpc.AuthServiceImplBase() {
                @Override
                public void hasAccess(AccessCheckRequest request, StreamObserver<AccessCheckResponse> responseObserver) {
                    AccessCheckResponse.Builder responseBuilder = AccessCheckResponse.newBuilder();

                    if (ACCESS.contains(request.getUserId())) {
                        responseBuilder.setAllowed(true);
                    }

                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }
            });
        }
    }

    @Test
    void thatGetWorksWhenUserHasAccess() {
        assertThat(testClient.get("/dataset-meta?name=a-dataset&operation=READ", Dataset.class).expect200Ok().body()).isEqualTo(CATALOG.get("a-dataset"));
    }

    @Test
    void thatGetReturns404WhenNoDatasetIsFound() {
        testClient.get("/dataset-meta?name=does-not-exist&operation=READ").expect404NotFound();
    }

    @Test
    void thatGetReturns400WhenNameIsMissing() {
        assertThat(testClient.get("/dataset-meta?operation=READ").expect400BadRequest().body()).isEqualTo("Expected 'name'");
    }

    @Test
    void testThatGetReturns400WhenOperationIsMissing() {
        assertThat(testClient.get("/dataset-meta?name=a-name").expect400BadRequest().body()).isEqualTo("Expected 'operation'");
    }
}
