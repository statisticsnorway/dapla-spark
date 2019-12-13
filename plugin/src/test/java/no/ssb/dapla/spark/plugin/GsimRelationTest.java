package no.ssb.dapla.spark.plugin;

import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;


public class GsimRelationTest {

    @Test
    public void testEquality() {
        // Equality on relation can be used by catalyst to optimize the physical plan.
        ArrayList<URI> uris = new ArrayList<>();

        uris.add(URI.create("lds+gsim://foo:1/bar"));
        uris.add(URI.create("lds+gsim://foo:2/bar"));
        uris.add(URI.create("lds+gsim://foo:3/bar"));
        uris.add(URI.create("lds+gsim://foo:4/bar"));

        ArrayList<URI> otherUris = new ArrayList<>(uris);
        otherUris.add(URI.create("lds+gsim://foo:5/bar"));

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(uris);
            GsimRelation relA = new GsimRelation(null, uris);
            Collections.shuffle(uris);
            GsimRelation relB = new GsimRelation(null, uris);
            Collections.shuffle(otherUris);
            GsimRelation relC = new GsimRelation(null, otherUris);

            assertThat(relA).isEqualTo(relB);
            assertThat(relA.hashCode()).isEqualTo(relB.hashCode());

            assertThat(relC).isNotEqualTo(relA);
            assertThat(relC.hashCode()).isNotEqualTo(relA.hashCode());
            
            assertThat(relC).isNotEqualTo(relB);
            assertThat(relC.hashCode()).isNotEqualTo(relB.hashCode());
        }
    }
}