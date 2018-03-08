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

import com.graphaware.horne.cdbg.sp.Persons;

import jline.internal.Log;


public class PersonsTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Persons.class);
	
	
	@Test
	public void shouldAllowUpdatingPerson() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher queries
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PERSON_DEL1_QUERY\", " + 
					"	query:\"Match (person:Person)-[]->(:Address)-[rel:HAS_ZIP]->(:Zip) where id(person) = {PersonId} delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PERSON_DEL2_QUERY\", " + 
					"	query:\"Match (person:Person)-[]->(:Address)-[rel:HAS_CITY]->(:City) where id(person) = {PersonId} delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PERSON_DEL3_QUERY\", " + 
					"	query:\"Match (person:Person)-[]->(:Address)-[rel:HAS_COUNTY]->(:County) where id(person) = {PersonId} delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PERSON_QUERY\", " + 
					"	query:\"Match (person:Person) where id(person) = {PersonId}  " + 
					"			set person.LegalEntityName={LegalEntityName}, person.FirstName={FirstName}, person.MiddleName={MiddleName}, person.LastName={LastName},  " + 
					"			person.Race={Race}, person.Ethnicity={Ethnicity}, person.Gender={Gender}, person.Disability={Disability},person.PreferredContactMethod={PreferredContactMethod}  " + 
					"			Merge (person)-[:HAS_PII]->(pii:PII) set pii.SSN = {SSN}, pii.DOB = {DOB}  " + 
					"			Merge (person)-[:HAS_ADDRESS]->(address:Address) set address.Street={Street},  address.Street1={Street1}, address.GeoFloodPlane={GeoFloodPlane}, address.GeoParcel={GeoParcel}  " + 
					"			WITH person, address Match (state:State{name:{State}})  " + 
					"			Merge (zip:Zip{zip:{Zip}})  " + 
					"			Merge (city:City{name:{City}})-[:IS_CITY_OF]->(state)  " + 
					"			Merge (county:County{name:{County}})-[:IS_COUNTY_OF]->(state)  " + 
					"			Merge (address)-[:HAS_CITY]->(city)  " + 
					"			Merge (address)-[:HAS_COUNTY]->(County)  " + 
					"			Merge (address)-[:HAS_ZIP]->(zip)  " + 
					"			with person, city Merge (person)-[:HAS_CONTACT]->(phone:Phone{Value:{Phone}})  " + 
					"			Merge (person)-[:HAS_CONTACT]->(mobile:Mobile{Value:{Mobile}})  " + 
					"			Merge (person)-[:HAS_CONTACT]->(email:Email{Value:{Email}})  " + 
					"			return id(person) AS PersonId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			session.run("CREATE (state:State{name:\"Texas\"}) RETURN id(state) AS StateId");
			Long personId = session.run("CREATE (person:Person) RETURN id(person) AS PersonId")
					.single()
					.get(0).asLong();
			
			// Update the person
			String json = session.run("CALL horne.cdbg.sp.updatePerson(\"Test Harness\", " + personId.toString() + ", \"Person Inc.\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"713-324-9384\",\"987-654-3210\",\"contact@lakeside.com\",\"Phone Call\",\"4909 Bissonnet Street\",\"Suite 200\",\"Bellaire\",\"Houston\",\"Texas\",\"77401\",\"\",\"\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);

			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("personId");

			// Check that the LegalEntityName was updated
			StatementResult result = session.run("MATCH (person:Person) WHERE id(person)=" + nodeId.toString() + " RETURN person.LegalEntityName AS LegalEntityName");
			assertThat(result.single().get("LegalEntityName").asString(), equalTo("Person Inc."));
		}
	}
	
	@Test
	public void shouldAllowCreatingPersonIncome() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_PERSON_INCOME_QUERY\", query:\"MATCH(person:Person) where id(person) = {PersonId} " + 
					"   with person Create (person)-[:HAS_INCOME]->(income:Income{Ammount:{Amount}, Source:{Source}, " + 
					"   VerificationStatus:{VerificationStatus}, VerificationDocument:{VerificationDocument}}) return id(income) AS IncomeId;\"}) RETURN id(new);");
		
			// Add the supporting metadata
			Long personId = session.run("CREATE (person:Person) RETURN id(person) AS PersonId")
					.single()
					.get(0).asLong();
		
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.createPersonIncome(\"Test Harness\", " + personId.toString() + ", \"50000\", \"bank\", \"verified\", \"doc\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
		
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long incomeId = resultArr.getJSONObject(0).getLong("incomeId");
		
			// Check the result
			StatementResult result = session.run("MATCH (person:Person)-[:HAS_INCOME]->(income:Income) WHERE id(person)=" + personId.toString() + " AND id(income) = " + incomeId.toString() + " RETURN income.Ammount AS Amount");
			assertThat(result.single().get("Amount").asString(), equalTo("50000"));
		}
	}
	
	@Test
	public void shouldAllowUpdatingPersonIncome() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_PERSON_INCOME_QUERY\", query:\"MATCH(person:Person)-[:HAS_INCOME]->(income:Income) " + 
					"   where id(person) = {PersonId} and id(income) = {IncomeId} set income.Ammount={Amount}, " + 
					"   income.Source={Source},income.VerificationStatus={VerificationStatus}, " + 
					"   income.VerificationDocument={VerificationDocument} return id(income) AS IncomeId;\"}) RETURN id(new);");
		
			// Add the supporting metadata
			Long personId = session.run("CREATE (person:Person) RETURN id(person) AS PersonId")
					.single()
					.get(0).asLong();
			Long incomeId = session.run("MATCH(person:Person) where id(person) = " + personId.toString() + 
					"   with person Create (person)-[:HAS_INCOME]->(income:Income{Ammount:50000, Source:\"Bank\", " + 
					"   VerificationStatus:\"Verified\", VerificationDocument:\"Statement\"}) return id(income) AS IncomeId;")
					.single()
					.get(0).asLong();
		
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updatePersonIncome(\"Test Harness\", " + personId.toString() + ", " + incomeId.toString() + ", \"150000\", \"bank\", \"verified\", \"doc\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
		
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			// Check the result
			StatementResult result = session.run("MATCH (person:Person)-[:HAS_INCOME]->(income:Income) WHERE id(person)=" + personId.toString() + " AND id(income) = " + incomeId.toString() + " RETURN income.Ammount AS Amount");
			assertThat(result.single().get("Amount").asString(), equalTo("150000"));
		}
	}

}
