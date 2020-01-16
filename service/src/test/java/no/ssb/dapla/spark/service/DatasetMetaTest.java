package no.ssb.dapla.spark.service;

import io.grpc.stub.StreamObserver;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckRequest;
import no.ssb.dapla.auth.dataset.protobuf.AccessCheckResponse;
import no.ssb.dapla.auth.dataset.protobuf.AuthServiceGrpc;
import no.ssb.dapla.catalog.protobuf.CatalogServiceGrpc.CatalogServiceImplBase;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetRequest;
import no.ssb.dapla.catalog.protobuf.GetByIdDatasetResponse;
import no.ssb.dapla.catalog.protobuf.MapNameToIdRequest;
import no.ssb.dapla.catalog.protobuf.MapNameToIdResponse;
import no.ssb.dapla.catalog.protobuf.SaveDatasetRequest;
import no.ssb.dapla.catalog.protobuf.SaveDatasetResponse;
import no.ssb.testing.helidon.GrpcMockRegistry;
import no.ssb.testing.helidon.GrpcMockRegistryConfig;
import no.ssb.testing.helidon.IntegrationTestExtension;
import no.ssb.testing.helidon.ResponseHelper;
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

    private static final Map<String, String> CATALOG_NAME_INDEX = new HashMap<>();

    static {
        CATALOG_NAME_INDEX.put("a-dataset", "123");
        CATALOG_NAME_INDEX.put("skatt2019.konto", "some_id");
        CATALOG_NAME_INDEX.put("maps_to_no_namespace", "non_existing_dataset");

        CATALOG.put(
                "123",
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
        CATALOG.put(
                "some_id",
                Dataset.newBuilder()
                        .setId(
                                DatasetId.newBuilder()
                                        .setId("some_id")
                        )
                        .build()
        );
    }

    private static final Set<String> ACCESS = Set.of("a-user");

    public static class DatasetMetaTestGrpcMockRegistry extends GrpcMockRegistry {
        public DatasetMetaTestGrpcMockRegistry() {
            add(new CatalogServiceImplBase() {
                @Override
                public void mapNameToId(MapNameToIdRequest request, StreamObserver<MapNameToIdResponse> responseObserver) {
                    MapNameToIdResponse.Builder responseBuilder = MapNameToIdResponse.newBuilder();

                    String id = CATALOG_NAME_INDEX.get(request.getName(0));
                    if (id != null) {
                        responseBuilder.setId(id);
                    }
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }

                @Override
                public void getById(GetByIdDatasetRequest request, StreamObserver<GetByIdDatasetResponse> responseObserver) {
                    GetByIdDatasetResponse.Builder responseBuilder = GetByIdDatasetResponse.newBuilder();

                    Dataset dataset = CATALOG.get(request.getId());
                    if (dataset != null) {
                        responseBuilder.setDataset(dataset);
                    }
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                }

                @Override
                public void save(SaveDatasetRequest request, StreamObserver<SaveDatasetResponse> responseObserver) {
                    responseObserver.onNext(SaveDatasetResponse.newBuilder().build());
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

    // CREATE
    @Test
    void thatCreateReturns400WhenMissingValuation() {
        assertThat(testClient.get("/dataset-meta?name=skatt2019.konto&operation=CREATE&userId=someuser").expect400BadRequest().body()).isEqualTo("Missing required query parameter 'valuation'");
    }

    @Test
    void thatCreateReturns400WhenMissingState() {
        assertThat(testClient.get("/dataset-meta?name=skatt2019.konto&operation=CREATE&userId=someuser&valuation=OPEN").expect400BadRequest().body()).isEqualTo("Missing required query parameter 'state'");
    }

    @Test
    void thatCreateReturnsDatasetWhenUserIsAllowed() {
        Dataset expected = Dataset.newBuilder().setId(DatasetId.newBuilder().setId("some_id").build()).build();
        assertThat(testClient.get("/dataset-meta?name=skatt2019.konto&operation=CREATE&userId=a-user&valuation=OPEN&state=RAW&proposedId=some_id", Dataset.class).expect200Ok().body()).isEqualTo(expected);
    }

    @Test
    void thatCreateReturns403WhenUserIsNotAllowed() {
        testClient.get("/dataset-meta?name=skatt2019.konto&operation=CREATE&userId=no_access&valuation=OPEN&state=RAW&proposedId=some_id").expect403Forbidden();
    }

    // READ
    @Test
    void thatReadReturns404IfDatasetDoesNotExist() {
        testClient.get("/dataset-meta?name=maps_to_no_namespace&operation=READ&userId=someuser").expect404NotFound();
    }

    @Test
    void thatReadReturns403WhenUserIsNotAllowed() {
        testClient.get("/dataset-meta?name=skatt2019.konto&operation=READ&userId=no-access").expect403Forbidden();
    }

    @Test
    void thatGetWorksWhenUserHasAccess() {
        assertThat(testClient.get("/dataset-meta?name=skatt2019.konto&operation=READ&userId=a-user", Dataset.class).expect200Ok().body()).isEqualTo(CATALOG.get("some_id"));
    }

    @Test
    void thatGetWorksWhenUserDoesntHaveAccess() {
        testClient.get("/dataset-meta?name=skatt2019.konto&operation=READ&userId=mr-no-access").expect403Forbidden();
    }

    @Test
    void thatGetReturns404WhenNoDatasetIsFound() {
        testClient.get("/dataset-meta?name=does-not-exist&operation=READ&userId=aUser").expect404NotFound();
    }

    @Test
    void thatGetReturns400WhenUserIdIsMissing() {
        String actualBody = testClient.get("/dataset-meta?name=a-dataset&operation=READ").expect400BadRequest().body();
        assertThat(actualBody).isEqualTo("Missing required query parameter 'userId'");
    }

    @Test
    void thatGetReturns400WhenNameIsMissing() {
        String actualBody = testClient.get("/dataset-meta?operation=READ&userId=aUser").expect400BadRequest().body();
        assertThat(actualBody).isEqualTo("Missing required query parameter 'name'");
    }

    @Test
    void testThatGetReturns400WhenOperationIsMissing() {
        String actualBody = testClient.get("/dataset-meta?name=a-name&userId=aUser").expect400BadRequest().body();
        assertThat(actualBody).isEqualTo("Missing required query parameter 'operation'");
    }

    @Test
    void thatPutWorks() {
        Dataset ds = Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("an_id").build())
                .setValuation(Dataset.Valuation.SHIELDED)
                .setState(Dataset.DatasetState.PRODUCT)
                .setPseudoConfig("config")
                .addLocations("f")
                .build();
        ResponseHelper<String> responseHelper = testClient.put("/dataset-meta", ds).expect200Ok();
        assertThat(responseHelper.response().headers().firstValue("Location").orElseThrow()).isEqualTo("/dataset-meta");
    }
}
