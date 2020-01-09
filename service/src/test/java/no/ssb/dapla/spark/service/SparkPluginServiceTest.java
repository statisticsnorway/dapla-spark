package no.ssb.dapla.spark.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(IntegrationTestExtension.class)
class SparkPluginServiceTest {

    @Inject
    TestClient testClient;

    @Test
    void thatLoadDatasetWorks() {
        assertThat(testClient.get("/sparkplugin/hello").expect200Ok().body()).isEqualTo("hello");
    }
}