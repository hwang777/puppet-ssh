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

import com.graphaware.horne.cdbg.sp.Cases;

import jline.internal.Log;


public class CasesTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Cases.class);
	
	
//	@Test
	public void shouldAllowCreatingCase() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"CREATE_CASE_QUERY\", query:\"Create (case:Case{ " + 
					"    Status:\\\"Active\\\",  " + 
					"    Language:{language},  " + 
					"    FEMAApplicationID:{FEMAApplicationId},  " + 
					"    IsSingleFamilyHome:{isSingleFamilyHome},  " + 
					"    IsPrimaryResidence:{isPrimaryResidence},  " + 
					"    IsADA:{isADA},  " + 
					"    HouseholdIncome:{householdIncome},  " + 
					"    ReceivedAdditionalFunds:{receivedAdditionalFunds},  " + 
					"    OwnsProperty:{ownsProperty}, OwnsLand:{ownsLand}, DidWaterEnterHome:{didWaterEnterHome}, DidWaterReachOutlets:{didWaterReachOutlets},  " + 
					"    HadInsurance:{hadInsurance}, HasLien:{hasLien}, Referral:{referral}, Voad:{voad}, IsVacant:{isVacant}, SurveyRating:{surveyRating} " + 
					"   }) " + 
					"   with case  " + 
					"   Match(origin:Origin) WHERE id(origin) = {originId}" + 
					"   with case, origin" + 
					"   Merge (case)-[:ORIGINATES_FROM]->(origin)" + 
					"   with case  " + 
					"   Match(prog:Program) WHERE id(prog) = {programId}" + 
					"   with case, prog" + 
					"   MATCH (prog)-[:HAS_PHASE]->(phase:Phase)" + 
					"   WITH case, prog, phase" + 
					"   ORDER BY phase.OrderNumber, phase.name" + 
					"   WITH case, prog, COLLECT(phase) AS phaseColl" + 
					"   UNWIND phaseColl[0..1] AS phase" + 
					"   WITH case, prog, phase" + 
					"   Merge (case)-[rel:IS_IN_PHASE]->(phase)   " + 
					"   with case, prog, phase" + 
					"   MATCH (prog)--(event:Event)" + 
					"   Merge (case)-[r:FILLED_FOR]->(prog) set case.CaseID = {propertyState}+'-'+event.name+'-'+id(case)  " + 
					"   with case, phase  " + 
					"   Create (case)-[:HAS_PROPERTY]->(property:Property{Type:{propertyType}, HasBridge:{propertyHasBridge}, YearBuilt:{propertyYearBuilt}})  " + 
					"   with case, property, phase  " + 
					"   Merge (property)-[:HAS_ADDRESS]->(address:Address{Street:{propertyStreet}})  " + 
					"   Merge (zip:Zip{zip:{propertyZip}}) Merge (city:City{name:{propertyCity}})  " + 
					"   Merge (address)-[:HAS_ZIP]->(zip)   " + 
					"   with address, city, case  " + 
					"   Match (state:State{name:{propertyState}}) Merge (address)-[:HAS_CITY]->(city) Merge (city)-[:IS_CITY_OF]->(state)  " + 
					"   Create(hh:Household)<-[:HAS_HOUSEHOLD]-(case) " + 
					"   RETURN id(case) as caseId;\"}) RETURN id(new);");
		
			// Add the supporting metadata
			session.run("CREATE (n:Origin{name:\"Kiosk\"})");
			session.run("CREATE (n:Program{name:\"Multipleresidency Owners Program 01\"})");
			session.run("CREATE (n:Phase{name:\"InTake\"})");
			session.run("CREATE (n:Event{name:\"Andrew\"})");
			session.run("CREATE (n:State{name:\"Texas\"})");
			session.run("MATCH (p:Program{name: \"Multipleresidency Owners Program 01\"}) WITH p "
					+ "MATCH (n:Event{name:\"Andrew\"}) WITH p,n MERGE (n)-[r:HAS_PROGRAM]->(p)");
		
			// Create a test node
			String json = session.run("CALL horne.cdbg.sp.createCaseFromStaging(\"{    \\\"case\\\": {        \\\"origin\\\": \\\"Kiosk\\\",        \\\"program\\\": \\\"Multipleresidency Owners Program 01\\\",        \\\"language\\\": \\\"text\\\",        \\\"FEMAApplicationID\\\": \\\"text\\\",        \\\"isSingleFamilyHome\\\": \\\"text\\\",        \\\"isPrimaryResidence\\\": \\\"text\\\",        \\\"isADA\\\": \\\"text\\\",        \\\"householdIncome\\\": \\\"text\\\",        \\\"receivedAdditionalFunds\\\": \\\"text\\\",        \\\"ownsProperty\\\": \\\"text\\\",        \\\"ownsLand\\\": \\\"text\\\",        \\\"didWaterEnterHome\\\": \\\"text\\\",        \\\"didWaterReachOutlets\\\": \\\"text\\\",        \\\"hadInsurance\\\": \\\"text\\\",        \\\"hasLien\\\": \\\"text\\\",        \\\"referral\\\": \\\"text\\\",        \\\"voad\\\": \\\"text\\\",        \\\"isVacant\\\": \\\"text\\\",        \\\"surveyRating\\\": \\\"text\\\",        \\\"notSubmittingNotifyMeOfOtherPrograms\\\": \\\"text\\\",        \\\"propertyType\\\": \\\"text\\\",        \\\"propertyHasBridge\\\": \\\"text\\\",        \\\"propertyYearBuilt\\\": \\\"text\\\",        \\\"propertyAddress\\\": \\\"text\\\",        \\\"propertyCity\\\": \\\"text\\\",        \\\"propertyCounty\\\": \\\"text\\\",        \\\"propertyState\\\": \\\"Texas\\\",        \\\"propertyZip\\\": \\\"text\\\"    },    \\\"person\\\": [{        \\\"personType\\\": \\\"Applicant\\\",        \\\"companyName\\\": \\\"text\\\",        \\\"firstName\\\": \\\"John\\\",        \\\"middleName\\\": \\\"text\\\",        \\\"lastName\\\": \\\"Smith\\\",        \\\"dateOfBirth\\\": \\\"text\\\",        \\\"SSN\\\": \\\"text\\\",        \\\"race\\\": \\\"text\\\",        \\\"ethnicity\\\": \\\"text\\\",        \\\"gender\\\": \\\"text\\\",        \\\"disability\\\": \\\"text\\\",        \\\"address\\\": \\\"text\\\",        \\\"city\\\": \\\"text\\\",        \\\"state\\\": \\\"Texas\\\",        \\\"zip\\\": \\\"text\\\",        \\\"email\\\": \\\"text\\\",        \\\"phonePrimary\\\": \\\"text\\\",        \\\"isPrimaryPhoneMobile\\\": \\\"text\\\",        \\\"phoneSecondary\\\": \\\"text\\\",        \\\"isSecondaryPhoneMobile\\\": \\\"text\\\",        \\\"preferredContactMethod\\\": \\\"text\\\",        \\\"taxReturn4506TRequested\\\": \\\"text\\\",        \\\"taxReturnFilingTypeCode\\\": \\\"text\\\",        \\\"taxReturnSpouseFirstName\\\": \\\"text\\\",        \\\"taxReturnSpouseLastName\\\": \\\"text\\\",        \\\"taxReturnSpouseSSN\\\": \\\"text\\\",        \\\"taxReturnFiledUnderPreviousAddress\\\": \\\"text\\\",        \\\"taxReturnPreviousAddressStreet\\\": \\\"text\\\",        \\\"taxReturnPreviousAddressCity\\\": \\\"text\\\",        \\\"taxReturnPreviousAddressStateCode\\\": \\\"text\\\",        \\\"taxReturnPreviousAddressZip\\\": \\\"text\\\"    }],    \\\"insuranceClaim\\\": [    {        \\\"insuranceType\\\": \\\"text\\\",        \\\"companyName\\\": \\\"text\\\",        \\\"companyPhone\\\": \\\"text\\\",        \\\"policyNumber\\\": \\\"text\\\",        \\\"claimNumber\\\": \\\"text\\\",        \\\"amount\\\": 23411    }]}\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
		
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long nodeId = resultArr.getJSONObject(0).getLong("caseId");
		
			// Find the node
			StatementResult result = session.run("MATCH (n:Case) WHERE n.CaseID = \"Texas-Andrew-" + nodeId.toString() + "\" RETURN id(n) AS caseId");
			Long checkNodeId = result.single().get("caseId").asLong();
			
			assertThat(checkNodeId, equalTo(nodeId));
		}
	}
	
	@Test
	public void shouldAllowUpdatingCase() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher queries
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASE_DEL1_QUERY\", " + 
					"	query:\"Match (case:Case) where id(case) = {CaseId} Match (case) -[rel:IS_IN_PHASE]-> (:Phase) delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");

			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASE_DEL2_QUERY\", " + 
					"	query:\"Match (case:Case) where id(case) = {CaseId} Match (case)-->(:Property)-->(:Address)-[rel:HAS_ZIP]->(:Zip) delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");

			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASE_DEL3_QUERY\", " + 
					"	query:\"Match (case:Case) where id(case) = {CaseId} Match (case)-->(:Property)-->(:Address)-[rel:HAS_CITY]->(:City) delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");

			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASE_DEL4_QUERY\", " + 
					"	query:\"Match (case:Case) where id(case) = {CaseId} Match (case)-->(:Household)-->(:Address)-[rel:HAS_ZIP]->(:Zip) delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");

			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASE_DEL5_QUERY\", " + 
					"	query:\"Match (case:Case) where id(case) = {CaseId} Match (case)-->(:Household)-->(:Address)-[rel:HAS_CITY]->(:City) delete rel return id(rel) AS RelationshipId;\"}) RETURN id(new);");
			
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASE_QUERY\", query:\"Match (case:Case) where id(case) = {CaseId}  " + 
					"			set case.Status={Status}, case.Referral={Referral}, case.Voad={Voad}, case.AppianProcessId={AppianProcessId}  " + 
					"			with case Match(prog:Program) where id(prog) = {ProgramId}  " + 
					"			Merge (case) -[r:FILED_FOR]->(prog)  " + 
					"			with case Match(phase:Phase) where id(phase)={PhaseId}  " + 
					"			Merge (case) -[rel:IS_IN_PHASE]-> (phase)  " + 
					"			with case Merge (case)-[:HAS_PROPERTY]->(property:Property)  " + 
					"			set property.Type={PropertyType}, property.OwnershipType={PropertyOwnershipType}  " + 
					"			with case, property Merge (property)-[:HAS_ADDRESS]->(address:Address)  " + 
					"			set address.street={PropertyStreet}, address.street1={PropertyStreet1}, address.GeoFloodPlane={PropertyGeoFloodPlane} , address.GeoParcel={PropertyGeoParcel}  " + 
					"			with case, address Match (state:State{name:{PropertyState}})  " + 
					"			Merge (zip:Zip{zip:{PropertyZip}})  " + 
					"			Merge (city:City{name:{PropertyCity}})-[:IS_CITY_OF]->(state)  " + 
					"			Merge (county:County{name:{PropertyCounty}})-[:IS_COUNTY_OF]->(state)  " + 
					"			Merge (address)-[:HAS_ZIP]->(zip)  " + 
					"			Merge (address)-[:HAS_CITY]->(city)  " + 
					"			Merge (address)-[:HAS_COUNTY]->(county)  " + 
					"			with case Merge (case)-[:HAS_HOUSEHOLD]->(hh:Household)  " + 
					"			set hh.HouseholdIncome={HouseholdIncome}, hh.HouseholdCount={HouseholdCount}, hh.AMICategory={AMICategory}  " + 
					"			with case, hh Merge (hh)-[:HAS_ADDRESS]->(haddress:Address)  " + 
					"			set haddress.street={HouseholdStreet}, haddress.street1={HouseholdStreet1} , haddress.GeoFloodPlane={HouseholdGeoFloodPlane} , haddress.GeoParcel={HouseholdGeoParcel}  " + 
					"			with case, haddress Match (hstate:State{name:{HouseholdState}})  " + 
					"			Merge (hzip:Zip{zip:{HouseholdZip}})  " + 
					"			Merge (hcity:City{name:{HouseholdCity}})-[:IS_CITY_OF]->(hstate)  " + 
					"			Merge (hcounty:County{name:{HouseholdCounty}})-[:IS_COUNTY_OF]->(hState)  " + 
					"			Merge (haddress)-[:HAS_ZIP]->(hzip)  " + 
					"			Merge (haddress)-[:HAS_CITY]->(hcity)  " + 
					"			Merge (haddress)-[:HAS_COUNTY]->(hcounty)  " + 
					"			return id(case) AS CaseId;\"}) RETURN id(new);");
			
			// Add the supporting metadata
			Long caseId = session.run("CREATE (case:Case{AppianProcessId:\"1000\"}) RETURN id(case) AS CaseId")
					.single()
					.get(0).asLong();
			Long programId = session.run("CREATE (program:Program{name:\"Test Program\"}) RETURN id(program) AS ProgramId")
					.single()
					.get(0).asLong();
			Long phaseId = session.run("CREATE (phase:Phase{name:\"Test Phase\"}) RETURN id(phase) AS PhaseId")
					.single()
					.get(0).asLong();
			
			// Update the case
			String json = session.run("CALL horne.cdbg.sp.updateCase(\"Test Harness\", " + caseId.toString() + "," + programId.toString() + "," + phaseId.toString() + ",\"Renters Program\",\"Intake\",\"Active\",\"TEST VOAD\",\"New Referral\",\"1001\",\"Ranch\",\"Owner\",\"123 Main Street\",\"\",\"Cedar Park\",\"Williamson\",\"Texas\",\"78613\",\"USA\",\"200000\",\"5\",\"Category1\",\"123 Second Street\",\"Suite 200\",\"Austin\",\"Travis\",\"Texas\",\"78613\",\"USA\",\"Flood Plane Code 01\",\"Parcel Code 01\",\"Flood Plane Code 02\",\"Parcel Code 02\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			// Check that the Appian Process Id was updated
			StatementResult result = session.run("MATCH (case:Case) WHERE id(case)=" + caseId.toString() + " RETURN case.AppianProcessId");
			assertThat(result.single().get(0).asString(), equalTo("1001"));
		}
	}
	
	@Test
	public void shouldAllowUpdatingCasePhase() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_CASEPHASE_QUERY\", query:\"MATCH(Case:Case)  where (Case.CaseID = {CaseTextId})   " + 
					"   Match (phase:Phase) where id(phase)= {PhaseId}   " + 
					"   Match (CasePhase:CasePhase) where id(CasePhase) = {CasePhaseId}  " + 
					"   with Case, phase, CasePhase  " + 
					"   Merge (CasePhase)-[:IS_FOR_PHASE]-> (phase)  " + 
					"   Merge (Case)-[:HAS_CASEPHASE]->(CasePhase)  " + 
					"   set CasePhase.Status = {CasePhaseStatus}  " + 
					"   return id(CasePhase) AS CasePhaseId;\"}) RETURN id(new);");
		
			// Add the supporting metadata
			session.run("CREATE (case:Case{CaseID: \"TEST CASE\"}) return id(case) AS CaseId");
			Long phaseId = session.run("CREATE (phase:Phase) RETURN id(phase) AS PhaseId")
					.single()
					.get(0).asLong();
			Long casePhaseId = session.run("CREATE (casephase:CasePhase) RETURN id(casephase) AS CasePhaseId")
					.single()
					.get(0).asLong();
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updateCasePhase(\"Test Harness\", \"TEST CASE\", " + phaseId.toString() + ", " + casePhaseId.toString() + ", \"Testing\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
					
			// Check the result
			StatementResult result = session.run("MATCH (case:Case) WHERE case.CaseID = \"TEST CASE\" "
					+ " MATCH (phase:Phase) WHERE id(phase) = " + phaseId.toString() 
					+ " WITH case, phase  MATCH (casephase:CasePhase)-[:IS_FOR_PHASE]->(phase:Phase) WITH casephase, case " 
					+ " MATCH (case)-[:HAS_CASEPHASE]->(casephase) RETURN id(casephase) AS CasePhaseId");
			assertThat(result.single().get("CasePhaseId").asLong(), equalTo(casePhaseId));
		}
	}

}
