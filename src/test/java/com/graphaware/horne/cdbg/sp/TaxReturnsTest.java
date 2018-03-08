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


public class TaxReturnsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(TaxReturns.class);

	
	@Test
	public void shouldAllowCreatingTaxReturn() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_TAX_RETURN_QUERY\", query:\"MATCH (person:Person) where id(person) = {personId}  " + 
					"   with person Create (taxReturn:TaxReturn{TaxReturn4506TRequested:{taxReturn4506TRequested},TaxReturnFilingTypeCode:{taxReturnFilingTypeCode}, " + 
					"   TaxReturnSpouseFirstName:{taxReturnSpouseFirstName},TaxReturnSpouseLastName:{taxReturnSpouseLastName},TaxReturnPreviousStreet:{previousStreet}, " + 
					"   TaxReturnPreviousCity:{previousCity},TaxReturnPreviousState:{previousState},TaxReturnPreviousZip:{previousZip}})  " + 
					"   Create (taxReturn)-[:HAS_SPOUSEPII]->(:PII{SSN:{spouseSSN}})  " + 
					"   Merge (person)-[:HAS_TAXRETURN]->(taxReturn)  " + 
					"   return id(taxReturn) AS taxReturnId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long personId = session.run("CREATE (n:Person{FirstName:\"John\"}) RETURN id(n) AS personId")
					.single()
					.get(0).asLong();
			
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.createTaxReturn(\"Test Harness\", " + personId.toString() + ",\"True\",\"001\",\"John\",\"Smith\",\"100 Main St.\",\"Austin\",\"Texas\",\"55555\",\"444-444-4444\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("taxReturnId");
		
			// Find the node
			StatementResult result = session.run("MATCH (n:TaxReturn) WHERE id(n) = " + nodeId.toString() + " RETURN id(n) AS taxReturnId");
			Long checkNodeId = result.single().get("taxReturnId").asLong();
			
			assertThat(checkNodeId, equalTo(nodeId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingTaxReturn() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_TAX_RETURN_QUERY\", query:\"MATCH (person:Person)-[:HAS_TAXRETURN]->(taxReturn:TaxReturn) " + 
					"   where id(person) = {PersonId} and id(taxReturn) = {TaxReturnId} " + 
					"   set taxReturn.TaxReturn4506TRequested={TaxReturn4506TRequested}, " + 
					"   taxReturn.TaxReturnFilingTypeCode={TaxReturnFilingTypeCode}, " + 
					"   taxReturn.TaxReturnSpouseFirstName={TaxReturnSpouseFirstName}, " + 
					"   taxReturn.TaxReturnSpouseLastName={TaxReturnSpouseLastName}, " + 
					"   taxReturn.TaxReturnPreviousStreet={PreviousStreet}, taxReturn.TaxReturnPreviousCity={PreviousCity}, " + 
					"   taxReturn.TaxReturnPreviousState={PreviousState},taxReturn.TaxReturnPreviousZip={PreviousZip} " + 
					"   Merge (taxReturn)-[:HAS_SPOUSEPII]->(pii:PII) set pii.SSN={SpouseSSN} return id(taxReturn) AS TaxReturnId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long personId = session.run("CREATE (person:Person{FirstName:\"John\"}) RETURN id(person) AS personId")
					.single()
					.get(0).asLong();
			Long taxReturnId = session.run("MATCH (person:Person) WHERE id(person) = " + personId.toString() + " WITH person " +
					"MERGE (person)-[:HAS_TAXRETURN]->(tax:TaxReturn{TaxReturnFilingTypeCode:\"TEST\"}) RETURN id(tax) AS taxReturnId;")
					.single()
					.get(0).asLong();
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updateTaxReturn(\"Test Harness\", " + personId.toString() + "," + taxReturnId.toString() + ", \"Home Owner's Insurance\",\"Geico\",\"Jean\",\"Smith\",\"1234 Campbell Road\",\"Houston\",\"Texas\",\"77777\",\"123-45-6789\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
		
			// Check the result
			StatementResult result = session.run("MATCH (tax:TaxReturn) WHERE id(tax) = " + taxReturnId.toString() + " RETURN tax.TaxReturnFilingTypeCode AS TaxReturnFilingTypeCode");
			assertThat(result.single().get("TaxReturnFilingTypeCode").toString(), equalTo("\"Geico\""));
		}
	}

}
