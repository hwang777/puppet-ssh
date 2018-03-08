package com.graphaware.horne.cdbg.sp;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;

import com.graphaware.horne.cdbg.sp.Agencies;

import jline.internal.Log;


public class AgenciesTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Agencies.class);
	
	
	@Test
	public void shouldAllowCreatingAgency() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_AGENCY_QUERY\", "
					+ "query:\"Create (node:Agency{name:{name}}) return id(node) as agencyId;\"}) RETURN id(new);");
			
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.createAgency(\"Test Harness\", \"Sample Agency\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);

			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("agencyId");

			// Find the node
			StatementResult result = session.run("MATCH (n:Agency) WHERE n.name = \"Sample Agency\" RETURN id(n) AS agencyId");
			Long checkNodeId = result.single().get("agencyId").asLong();

			assertThat(checkNodeId, equalTo(nodeId));
		}
	}

}
