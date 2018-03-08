
package com.graphaware.horne.cdbg.sp.api;

import com.graphaware.test.integration.GraphAwareIntegrationTest;
import com.graphaware.test.unit.GraphUnit;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.util.Collections;

import static com.graphaware.test.unit.GraphUnit.assertSameGraph;
import static org.neo4j.graphdb.Label.label;


/**
 * Integration test for {@link TimeTreeApi}.
 */
public class AddressesApiTest extends GraphAwareIntegrationTest {

/*	
    @Test
    public void testValidateAddressl() throws IOException {
    	try (Transaction tx = getDatabase().beginTx()) {
            //id = getDatabase().createNode().getId();
            tx.success();
        }


        httpClient.get(getUrl() + "validate/28623 Blue Holly Lane/7494?city=katy&state=tx", HttpStatus.SC_OK);
    }

 */ 	
	
    private String getUrl() {
        return baseUrl() + "/validateaddr/";
    }
}
