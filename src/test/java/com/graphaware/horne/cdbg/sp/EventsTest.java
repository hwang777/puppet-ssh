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

import com.graphaware.horne.cdbg.sp.Events;

import jline.internal.Log;


public class EventsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Events.class);
	
	@Test
	public void shouldAllowCreatingEvent() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_EVENT_QUERY\", query:\"Create (node:Event{name:{eventName}, Type:{eventType}}) return id(node) as eventId;\"}) RETURN id(new);");
		
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.createEvent(\"Test Harness\", \"Hurricane Irma\", \"Hurricane\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
		
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("eventId");
		
			// Find the node
			StatementResult result = session.run("MATCH (n:Event) WHERE n.name = \"Hurricane Irma\" RETURN id(n) AS eventId");
			Long checkNodeId = result.single().get("eventId").asLong();

			assertThat(checkNodeId, equalTo(nodeId));
		}
	}

}
