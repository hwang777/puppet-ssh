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

import jline.internal.Log;


public class CommunicationsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Communications.class);

	
	@Test
	public void shouldAllowCreatingCommunication() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_COMMUNICATION_QUERY\", query:\"Create (node:Communication{CreatedTime:timestamp()}) return id(node) AS CommunicationId;\"}) RETURN id(new);");
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.createCommunication(\"Test Harness\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);

			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("communicationId");

			// Check the result
			StatementResult result = session.run("MATCH (comm:Communication) WHERE id(comm) = " + nodeId.toString() + " RETURN id(comm) AS CommunicationId");
			Long checkNodeId = result.single().get("CommunicationId").asLong();

			assertThat(checkNodeId, equalTo(nodeId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingCommunication() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_COMMUNICATION_QUERY\", query:\"MATCH (node:Communication) where id(node) = {CommunicationId}  " + 
					"   set node.SentFrom={SentFrom}, node.SentTo={SentTo}, node.Method={Method}, node.CreatedBy={CreatedBy}, node.DocId={DocId}, node.Description={Description}, node.Context={Context}, node.Content={Content}  " + 
					"   with node Match (case:Case{CaseID:{CaseTextId}}) Merge (case)-[:HAS_COOMUNICATION]->(node)  " + 
					"   return id(node) as CommunicationId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			session.run("CREATE (case:Case{CaseID:\"TEST\"}) RETURN id(case) AS CaseId");
			Long communicationId = session.run("CREATE (comm:Communication) RETURN id(comm) AS CommunicationId")
					.single()
					.get(0).asLong();
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updateCommunication(\"Test Harness\", " + communicationId.toString() + ", \"sender\", \"sendee\", \"email\", \"jdoe\", \"555\", \"some type of document\", \"context\", \"content\", \"TEST\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);

			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("communicationId");

			// Check the result
			StatementResult result = session.run("MATCH (comm:Communication) WHERE id(comm) = " + nodeId.toString() + " RETURN comm.SentFrom AS SentFrom");
			assertThat(result.single().get(0).toString(), equalTo("\"sender\""));
		}
	}

}
