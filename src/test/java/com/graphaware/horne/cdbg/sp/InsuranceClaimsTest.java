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

import com.graphaware.horne.cdbg.sp.InsuranceClaims;

import jline.internal.Log;


public class InsuranceClaimsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(InsuranceClaims.class);

	
	@Test
	public void shouldAllowUpdatingInsuranceClaim() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_INSURANCE_CLAIM_QUERY\", query:\"MATCH (case:Case)-[:HAS_INSURANCECLAIM]->(ins:InsuranceClaim)  " + 
					"   where id(case) = {CaseId} and id(ins) = {InsuranceClaimId} set ins.Type={Type}, ins.Provider={Provider}, " + 
					"   ins.Phone={Phone},ins.PolicyNumber={PolicyNumber},ins.ClaimNumber={ClaimNumber}, ins.Amount={Amount} ,  " + 
					"   ins.VerificationStatus={VerificationStatus} return id(ins) AS insuranceClaimId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long caseId = session.run("CREATE (case:Case{name: \"TEST CASE\"}) return id(case) AS caseId")
					.single()
					.get(0).asLong();
			Long claimId = session.run("MATCH (case:Case) WHERE id(case) = " + caseId.toString() + " WITH case MERGE (case)-[:HAS_INSURANCECLAIM]->(claim:InsuranceClaim{ClaimNumber:\"C-3242123345\"}) RETURN id(claim) AS claimId")
					.single()
					.get(0).asLong();
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updateInsuranceClaim(\"Test Harness\", " + caseId.toString() + ", " + claimId.toString() + ", \"Home Owner's Insurance\", \"Progressive\", \"345-234-5232\", \"P-191432454\", \"C-3242123345\", \"90000\", \"Pending Verification\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);

			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			// Find the node
			StatementResult result = session.run("MATCH (claim:InsuranceClaim) WHERE id(claim) = " + claimId.toString() + " RETURN claim.VerificationStatus AS VerificationStatus");
			assertThat(result.single().get("VerificationStatus").asString(), equalTo("Pending Verification"));
		}
	}
	
	@Test
	public void shouldAllowCreatingInsuranceClaim() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_INSURANCE_CLAIM_QUERY\", query:\"MATCH (case:Case) " + 
					"   where id(case) = {caseId} with case Create (ins:InsuranceClaim{Type:{type}, " + 
					"   Provider:{provider},Phone:{phone},PolicyNumber:{policyNumber}," + 
					"   ClaimNumber:{claimNumber},Amount:{amount}})  Create (case)-[:HAS_INSURANCECLAIM]->(ins) " + 
					"   return id(ins) AS insuranceClaimId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			StatementResult result = session.run("CREATE (n:Case{name:\"TestCase\"}) RETURN id(n) AS caseId");
			Long caseId = result.single().get("caseId").asLong();
			
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.createInsuranceClaim(\"Test Harness\", " + caseId + ", \"Home Owner's Insurance\", \"Progressive\", \"345-234-5232\", \"P-191432454\", \"C-3242123345\", 90000)")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("insuranceClaimId");
		
			// Find the node
			result = session.run("MATCH (n:InsuranceClaim) WHERE id(n) = " + nodeId.toString() + " RETURN id(n) AS insuranceClaimId");
			Long checkNodeId = result.single().get("insuranceClaimId").asLong();
			
			assertThat(checkNodeId, equalTo(nodeId));
		}
	}

}
