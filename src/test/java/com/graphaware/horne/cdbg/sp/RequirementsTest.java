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

import com.graphaware.horne.cdbg.sp.Requirements;

import jline.internal.Log;


public class RequirementsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Requirements.class);
	
	@Test
	public void shouldAllowCreatingRequirement() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_REQUIREMENT_QUERY\", query:\"Create (node:Requirement) return id(node) as requirementId;\"}) RETURN id(new);");
		
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.createRequirement(\"Test Harness\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
		
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("requirementId");
		
			// Find the node
			StatementResult result = session.run("MATCH (n:Requirement) WHERE id(n) = " + nodeId.toString() + " RETURN id(n) AS requirementId");
			Long checkNodeId = result.single().get("requirementId").asLong();

			assertThat(checkNodeId, equalTo(nodeId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingProgramRequirement() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PROGRAM_REQUIREMENT_QUERY\", query:\"MATCH (program:Program) where id(program)= {ProgramId}  " + 
					"   with program Match(phase:Phase) where id(phase) = {PhaseId}  " + 
					"   with program, phase Match (req:Requirement) where id(req)= {RequirementId}  " + 
					"   with program, phase, req Merge (phase)-[:REQUIRES]->(req)  " + 
					"   set req.name={RequirementName}, req.Type = {RequirementType}, req.Mapping = {RequirementMapping},  " + 
					"   req.ComparisonOperator = {ComparisonOperator}, req.ComparisonValue = {ComparisonValue}  " + 
					"   Return id(req) as RequirementId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long programId = session.run("CREATE (n:Program{name:\"Test Program\"}) RETURN id(n) AS programId")
					.single()
					.get(0).asLong();
			Long phaseId = session.run("CREATE (n:Phase{name:\"Test Phase\"}) RETURN id(n) AS phaseId")
					.single()
					.get(0).asLong();
			session.run("MATCH (program:Program) where id(program)= " + programId.toString() + 
					"   with program Match(phase:Phase) where id(phase) = " + phaseId.toString() + 
					"   with program, phase Merge (program)-[:HAS_PHASE]->(phase)  " + 
					"   set phase.name= \"TEST PHASE\"" + 
					"   Return id(program) as ProgramId, id(phase) as PhaseId");
			Long requirementId = session.run("CREATE (node:Requirement{name: \"NEW REQ\"}) RETURN id(node) AS requirementId")
					.single()
					.get(0).asLong();
			
			// Link the requirement to the program
			String json = session.run("CALL horne.cdbg.sp.updateProgramRequirement(\"Test Harness\", " + programId.toString() + ", " + phaseId.toString() + ", " + requirementId.toString() + ", \"Applicant First and Last Name\", \"Data\", \"Applicant First and Last Name\", \"\", \"\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			// Find the relationship
			StatementResult result = session.run("MATCH (program:Program) WHERE id(program) = " + programId.toString() + 
					" WITH program MATCH (program)-[:HAS_PHASE]->(phase:Phase) " +
					" WITH phase MATCH (phase)-[:REQUIRES]->(req:Requirement) " +
					" RETURN id(req) AS requirementId LIMIT 1");
			
			assertThat(result.single().get("requirementId").asLong(), equalTo(requirementId));
		}
	}
	
	@Test
	public void shouldAllowCreatingRequirementData() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_REQUIREMENT_DATA_QUERY\", query:\"Create (n:RequirementData) Return id(n) as RequirementDataId;\"}) RETURN id(new);");
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.createRequirementData(\"Test Harness\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("requirementDataId");
			
			// Check the result
			StatementResult result = session.run("MATCH (reqdata:RequirementData) WHERE id(reqdata) = " + nodeId.toString() + " RETURN id(reqdata) AS RequirementDataId");
			
			assertThat(result.single().get("RequirementDataId").asLong(), equalTo(nodeId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingRequirementData() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_REQUIREMENT_DATA_QUERY\", query:\"MATCH (Requirement:Requirement) where id(Requirement) = {RequirementId}  " + 
					"   Match (CasePhase:CasePhase) where id(CasePhase) = {CasePhaseId}  " + 
					"   match (data:RequirementData) where id(data) = {RequirementDataId}  " + 
					"   Merge (CasePhase)-[:HAS_REQUIREMENT_DATA]->(data)  " + 
					"   Merge (data)-[:IS_FOR_REQUIREMENT]->(Requirement)  " + 
					"   set data.Value = {RequirementDataValue}, data.Status = {RequirementDataStatus}  " + 
					"   return id(data) as RequirementDataId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long requirementId = session.run("CREATE (req:Requirement) RETURN id(req) AS RequirementId")
					.single()
					.get(0).asLong();
			Long casePhaseId = session.run("CREATE (n:CasePhase) RETURN id(n) AS CasePhaseId")
					.single()
					.get(0).asLong();
			Long requirementDataId = session.run("CREATE (req:RequirementData) RETURN id(req) AS RequirementDataId")
					.single()
					.get(0).asLong();
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updateRequirementData(\"Test Harness\", " + requirementId.toString() + ", " + casePhaseId.toString() + ", " + requirementDataId.toString() + ", \"Testing\", \"Pending\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			// Check the result
			StatementResult result = session.run("MATCH (req:RequirementData) WHERE id(req) = " + requirementDataId.toString() + " RETURN req.Status AS Status");
			assertThat(result.single().get(0).asString(), equalTo("Pending"));
		}
	}

}
