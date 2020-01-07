package no.ssb.dapla.spark.service.health;

import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.webserver.Routing;
import io.helidon.webserver.Service;
import io.helidon.webserver.WebServer;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;


public class Health implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(Health.class);

    private final Supplier<WebServer> webServer;

    public Health(Supplier<WebServer> webServer) {
        this.webServer = webServer;
    }

    @Override
    public void update(Routing.Rules rules) {
        LOG.info("update {}", rules);
        rules.register(HealthSupport.builder()
                .addLiveness(HealthChecks.healthChecks())
                .addLiveness(() -> HealthCheckResponse.named("LivenessCheck")
                        .up()
                        .withData("time", System.currentTimeMillis())
                        .build())
                .addReadiness(() -> HealthCheckResponse.named("ReadinessCheck")
                        .state(webServer.get().isRunning())
                        .build())
                .build());
    }
}
