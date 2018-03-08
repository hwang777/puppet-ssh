package com.graphaware.horne.cdbg.sp;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

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


public class ProgramsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Programs.class);
	
	@Test
	public void shouldAllowUpdatingProgram() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE ).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PROGRAM_QUERY\", query:\"MATCH(program:Program) where id(program) = {ProgramId}  " + 
					"   Match (event:Event{name:{EventName}})  " + 
					"   Match (agent:Agency{name:{AgencyName}}) " + 
					"   Merge (program)<-[:HAS_PROGRAM]-(event)  " + 
					"   Merge (program)-[:HAS_AGENCY]->(agent)  " + 
					"   set program.name = {ProgramName}, program.State={ProgramState}, program.Type = {ProgramType}, " + 
					"   program.EligibleCensusTract = {EligibleCensusTract} Return id(program) as programId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			session.run("CREATE (n:Event{name: \"TestEvent\"})");
			session.run("CREATE (n:Agency{name: \"TestAgency\"})");
			Long programId = session.run("CREATE (n:Program{name:\"Test Program\"}) RETURN id(n) AS programId")
					.single()
					.get(0).asLong();
			
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.updateProgram(\"Test Harness\", " + programId.toString() + ", \"TestEvent\", \"TestAgency\", \"NewProgram\", \"type\", \"state\", \"census\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
		
			// Check the node
			StatementResult result = session.run("MATCH (n:Program) WHERE id(n) = " + programId.toString() + " RETURN n.name AS programName");
			String programName = result.single().get("programName").toString();
		
			assertThat(programName, equalTo("\"NewProgram\""));
		}
	}
	
	@Test
	public void shouldAllowUpdatingProgramCounty() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE ).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PROGRAM_COUNTY_QUERY\", query:\"MATCH (program:Program) where id(program)= {ProgramId}  " + 
					"   with program Match(county:County) where id(county) = {CountyId}  " + 
					"   with program, county Merge (program)-[:COVERS_COUNTY]->(county)  " + 
					"   Return id(program) as ProgramId, id(county) as CountyId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long programId = session.run("CREATE (n:Program{name:\"Test Program\"}) RETURN id(n) AS programId")
					.single()
					.get(0).asLong();
			Long countyId = session.run("CREATE (n:County{name:\"Test County\"}) RETURN id(n) AS countyId")
					.single()
					.get(0).asLong();
			
			// Link the county to the program
			String json = session.run("CALL horne.cdbg.sp.updateProgramCounty(\"Test Harness\", " + programId + ", " + countyId + ")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
		
			// Check the node
			StatementResult result = session.run("MATCH (program:Program)-[:COVERS_COUNTY]->(county:County) WHERE id(program) = " +
						programId.toString() + " AND id(county) = " + countyId.toString() + " RETURN id(program) AS programId");
			Long checkProgramId = result.single().get("programId").asLong();
		
			assertThat(checkProgramId, equalTo(programId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingProgramCity() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE ).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PROGRAM_CITY_QUERY\", query:\"MATCH (program:Program) where id(program)= {ProgramId}  " + 
					"   with program Match(city:City) where id(city) = {CityId}  " + 
					"   with program, city Merge (program)-[:COVERS_CITY]->(city)  " + 
					"   Return id(program) as ProgramId, id(city) as CityId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long programId = session.run("CREATE (n:Program{name:\"Test Program\"}) RETURN id(n) AS programId")
					.single()
					.get(0).asLong();
			Long cityId = session.run("CREATE (n:City{name:\"Test City\"}) RETURN id(n) AS cityId")
					.single()
					.get(0).asLong();
			
			// Link the nodes
			String json = session.run("CALL horne.cdbg.sp.updateProgramCity(\"Test Harness\", " + programId.toString() + ", " + cityId.toString() + ")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
		
			// Find the relationship
			StatementResult result = session.run("MATCH (program:Program) WHERE id(program) = " + programId.toString() + " WITH program MATCH (program)-[:COVERS_CITY]->(city:City) RETURN id(city) AS cityId LIMIT 1");
			
			assertThat(result.single().get("cityId").asLong(), equalTo(cityId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingProgramPhase() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE ).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PROGRAM_PHASE_QUERY\", query:\"MATCH (program:Program) where id(program)= {ProgramId}  " + 
					"   with program Match(phase:Phase) where id(phase) = {PhaseId}  " + 
					"   with program, phase Merge (program)-[:HAS_PHASE]->(phase)  " + 
					"   set phase.name={PhaseName}  " + 
					"   Return id(program) as ProgramId, id(phase) as PhaseId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long programId = session.run("CREATE (n:Program{name:\"Test Program\"}) RETURN id(n) AS programId")
					.single()
					.get(0).asLong();
			Long phaseId = session.run("CREATE (n:Phase{name:\"Test Phase\"}) RETURN id(n) AS phaseId")
					.single()
					.get(0).asLong();
			
			// Link the nodes
			String json = session.run("CALL horne.cdbg.sp.updateProgramPhase(\"Test Harness\", " + programId.toString() + ", " + phaseId.toString() + ", \"Test Phase\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
		
			// Find the relationship
			StatementResult result = session.run("MATCH (program:Program) WHERE id(program) = " + programId.toString() + " WITH program MATCH (program)-[:HAS_PHASE]->(phase:Phase) RETURN id(phase) AS phaseId LIMIT 1");
			
			assertThat(result.single().get("phaseId").asLong(), equalTo(phaseId));
		}
	}

}
