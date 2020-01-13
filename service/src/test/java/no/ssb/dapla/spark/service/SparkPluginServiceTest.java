package no.ssb.dapla.spark.service;

import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class SparkPluginServiceTest {

    @Inject
    TestClient testClient;

    //@Test
    void thatLoadDatasetWorks() {
        assertThat(testClient.get("/sparkplugin?name=a/b/c&operation=READ").expect200Ok().body())
                .isEqualTo("{\"id\": 123, \"locations\": [\"f1\", \"f2\"]}");
    }
}
