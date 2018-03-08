package com.graphaware.horne.cdbg.sp;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class Cases {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();	
	
    public static class Output {
    	public String result;
    }     
    
    public static class CreateCase
    {
        public Long caseId;
        public String caseTextId;
  
        public CreateCase (Node caseNode) 
        {
            // Set the case attributes
            this.caseId = new Long(commonUtils.getProperty(caseNode, "id"));
            this.caseTextId = commonUtils.getProperty(caseNode, "CaseID");
        }
    }
    

	/*
	 *  Creates a new case
	 */
	private CreateCase createCase(String submittedBy,
			Long programId,
			String language,
			String FEMAApplicationId,
			String isSingleFamilyHome,
			String isPrimaryResidence,
			String isADA,
			String householdIncome,
			String receivedAdditionalFunds,
			String ownsProperty,
			String ownsLand,
			String didWaterEnterHome,
			String didWaterReachOutlets,
			String hadInsurance,
			String hasLien,
			String referral,
			String voad,
			String isVacant,
			String surveyRating,
			String propertyType,
			String propertyHasBridge,
			String propertyYearBuilt,
			String propertyStreet,
			String propertyCity,
			String propertyCounty,
			String propertyState,
			String propertyZip) throws Throwable
	{
		CreateCase caseObj = null;
		
		// Get the cypher query
		String cypher = commonUtils.getCypherQuery(db, "CREATE_CASE_QUERY");
			
		// Set up the parameters
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("programId", programId);
		params.put("language", language);
		params.put("FEMAApplicationId", FEMAApplicationId);
		params.put("isSingleFamilyHome", isSingleFamilyHome);
		params.put("isPrimaryResidence", isPrimaryResidence);
		params.put("isADA", isADA);
		params.put("householdIncome", householdIncome);
		params.put("receivedAdditionalFunds", receivedAdditionalFunds);
		params.put("ownsProperty", ownsProperty);
		params.put("ownsLand", ownsLand);
		params.put("didWaterEnterHome", didWaterEnterHome);
		params.put("didWaterReachOutlets", didWaterReachOutlets);
		params.put("hadInsurance", hadInsurance);
		params.put("hasLien", hasLien);
		params.put("referral", referral);
		params.put("voad", voad);
		params.put("isVacant", isVacant);
		params.put("surveyRating", surveyRating);
		params.put("propertyType", propertyType);
		params.put("propertyHasBridge", propertyHasBridge);
		params.put("propertyYearBuilt", propertyYearBuilt);
		params.put("propertyStreet", propertyStreet);
		params.put("propertyCity", propertyCity);
		params.put("propertyState", propertyState);
		params.put("propertyZip", propertyZip);

		// Run the cypher query
		Result result = db.execute(cypher, params);
		if(!result.hasNext()) throw new Exception("There was an error creating the case");
	
		// Process the results
		while(result.hasNext())
		{
			Map<String, Object> row = result.next();            
			
			Node caseNode = commonUtils.checkForNullNode(db, row.get("caseId"));
            caseObj = new CreateCase(caseNode);
            
			break;
		}
		
		// ************************************************************
		//    Link in the county (TODO: Lookup county from zip)
		// ************************************************************
		
		if(!propertyCounty.equals("")) {
			cypher = commonUtils.getCypherQuery(db, "CREATE_CASE_COUNTY_QUERY");
			
			params = new HashMap<String, Object>();
			params.put("caseId", caseObj.caseId);
			params.put("propertyStreet", propertyStreet);
			params.put("propertyCounty", propertyCounty);
			params.put("propertyState", propertyState);
			
			result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error linking the case to the property county");
		}
	
		return caseObj;
	}
	
	@Procedure(name = "horne.cdbg.sp.createCaseFromStaging", mode = WRITE)
	public Stream<Output> createCaseFromStaging(@Name("request") String request) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
	
		JSONObject jsonResultObj = new JSONObject();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Persons personUtils = new Persons(db, log);
    	TaxReturns taxReturnUtils = new TaxReturns(db, log);
    	InsuranceClaims insuranceClaimUtils = new InsuranceClaims(db, log);
 	
    	// Parse out the json input
    	JSONObject jsonInput = new JSONObject(request);
    	
    	Transaction tx = db.beginTx();
		
		try {
			String submittedBy = "";

    		// *******************************************************
			//    Process the case
    		// *******************************************************
			if(!jsonInput.has("case")) throw new Exception("Request is missing case element");
			JSONObject caseObj = jsonInput.getJSONObject("case");
			
			String origin = commonUtils.getJSONStringProperty(caseObj, "origin");
			String program = commonUtils.getJSONStringProperty(caseObj, "program");
			String language = commonUtils.getJSONStringProperty(caseObj, "language");
			String FEMAApplicationId = commonUtils.getJSONStringProperty(caseObj, "FEMAApplicationID");
			String isSingleFamilyHome = commonUtils.getJSONStringProperty(caseObj, "isSingleFamilyHome");
			String isPrimaryResidence = commonUtils.getJSONStringProperty(caseObj, "isPrimaryResidence");
			String isADA = commonUtils.getJSONStringProperty(caseObj, "isADA");
			String householdIncome = commonUtils.getJSONStringProperty(caseObj, "householdIncome");
			String receivedAdditionalFunds = commonUtils.getJSONStringProperty(caseObj, "receivedAdditionalFunds");
			String ownsProperty = commonUtils.getJSONStringProperty(caseObj, "ownsProperty");
			String ownsLand = commonUtils.getJSONStringProperty(caseObj, "ownsLand");
			String didWaterEnterHome = commonUtils.getJSONStringProperty(caseObj, "didWaterEnterHome");
			String didWaterReachOutlets = commonUtils.getJSONStringProperty(caseObj, "didWaterReachOutlets");
			String hadInsurance = commonUtils.getJSONStringProperty(caseObj, "hadInsurance");
			String hasLien = commonUtils.getJSONStringProperty(caseObj, "hasLien");
			String referral = commonUtils.getJSONStringProperty(caseObj, "referral");
			String voad = commonUtils.getJSONStringProperty(caseObj, "voad");
			String isVacant = commonUtils.getJSONStringProperty(caseObj, "isVacant");
			String surveyRating = commonUtils.getJSONStringProperty(caseObj, "surveyRating");
			//String notSubmittingNotifyMeOfOtherPrograms = commonUtils.getJSONStringProperty(caseObj, "notSubmittingNotifyMeOfOtherPrograms");
			String propertyType = commonUtils.getJSONStringProperty(caseObj, "propertyType");
			String propertyHasBridge = commonUtils.getJSONStringProperty(caseObj, "propertyHasBridge");
			String propertyYearBuilt = commonUtils.getJSONStringProperty(caseObj, "propertyYearBuilt");
			String propertyAddress = commonUtils.getJSONStringProperty(caseObj, "propertyAddress");
			String propertyCity = commonUtils.getJSONStringProperty(caseObj, "propertyCity");
			String propertyCounty = commonUtils.getJSONStringProperty(caseObj, "propertyCounty");
			String propertyState = commonUtils.getJSONStringProperty(caseObj, "propertyState");
			String propertyZip = commonUtils.getJSONStringProperty(caseObj, "propertyZip");
			
    		// Validate the required attributes
			if(program.equals("")) throw new Exception("The program is required");
    		if(propertyAddress.equals("")) throw new Exception("The propertyAddress is required");
    		if(propertyCity.equals("")) throw new Exception("The propertyCity is required");
    		if(propertyState.equals("")) throw new Exception("The propertyState is required");
    		if(propertyZip.equals("")) throw new Exception("The propertyZip is required");
		
			// Validate the program
    		Map<String, Object> params = new HashMap<String, Object>();
			params.put("program", program);
			Result result = db.execute("MATCH (program:Program{name:{program}}) RETURN id(program) AS programId", params);
    		if(!result.hasNext()) throw new Exception("The program was not found");
    		Long programId = commonUtils.getRowValueLong(result.next(), "programId");
   		
    		// Validate the property state
			params = new HashMap<String, Object>();
			params.put("state", propertyState);
			result = db.execute("MATCH (state:State{name:{state}}) RETURN id(state) AS stateId", params);
    		if(!result.hasNext()) throw new Exception("The state was not found");
	
    		// Create the case
    		CreateCase caseResult = createCase(submittedBy,
    				programId,
				    language,
				    FEMAApplicationId,
				    isSingleFamilyHome,
				    isPrimaryResidence,
				    isADA,
				    householdIncome,
				    receivedAdditionalFunds,
				    ownsProperty,
				    ownsLand,
				    didWaterEnterHome,
				    didWaterReachOutlets,
				    hadInsurance,
				    hasLien,
				    referral,
				    voad,
				    isVacant,
				    surveyRating,
				    propertyType,
				    propertyHasBridge,
				    propertyYearBuilt,
				    propertyAddress,
				    propertyCity,
				    propertyCounty,
				    propertyState,
				    propertyZip);
	
			// Link the case to the origin (we're going to create the origin if it doesn't exist)
    		if(!origin.equals("")) {
    			
    			// Look up the origin - create the origin if it doesn't exist
				params = new HashMap<String, Object>();
				params.put("origin", origin);
				result = db.execute("MATCH (origin:Origin{name:{origin}}) RETURN id(origin) AS originId", params);
	    		if(!result.hasNext()) {
	    			result = db.execute("CREATE (origin:Origin{name:{origin}}) RETURN id(origin) AS originId", params);
	    			if(!result.hasNext()) throw new Exception("There was an error adding the Origin");
	    		}
	    		Long originId = commonUtils.getRowValueLong(result.next(), "originId");
	    		
	    		// Link the case to the origin
        		String cypher = commonUtils.getCypherQuery(db, "CREATE_CASE_ORIGIN_QUERY");
				params = new HashMap<String, Object>();
				params.put("caseId", caseResult.caseId);
				params.put("originId", originId);
				result = db.execute(cypher, params);
				if(!result.hasNext()) throw new Exception("There was an error linking the origin to the case");
	    		
    		}
    		
    		
    		// *******************************************************
    		//    Process the persons
    		// *******************************************************
    		
    		if(jsonInput.has("person")) {
	    		JSONArray persons = new JSONArray(jsonInput.getJSONArray("person").toString());
	    		
		    	Iterator<Object> personsIter = persons.iterator();
		    	while(personsIter.hasNext()) {
		    		JSONObject person = new JSONObject(personsIter.next().toString());
	
		    		String personType = commonUtils.getJSONStringProperty(person, "personType");
		            String companyName = commonUtils.getJSONStringProperty(person, "companyName");
		            String firstName = commonUtils.getJSONStringProperty(person, "firstName");
		            String middleName = commonUtils.getJSONStringProperty(person, "middleName");
		            String lastName = commonUtils.getJSONStringProperty(person, "lastName");
		            String dateOfBirth = commonUtils.getJSONStringProperty(person, "dateOfBirth");
		            String SSN = commonUtils.getJSONStringProperty(person, "SSN");
		            String race = commonUtils.getJSONStringProperty(person, "race");
		            String ethnicity = commonUtils.getJSONStringProperty(person, "ethnicity");
		            String gender = commonUtils.getJSONStringProperty(person, "gender");
		            String disability = commonUtils.getJSONStringProperty(person, "disability");
		            String address = commonUtils.getJSONStringProperty(person, "address");
		            String city = commonUtils.getJSONStringProperty(person, "city");
		            String state = commonUtils.getJSONStringProperty(person, "state");
		            String zip = commonUtils.getJSONStringProperty(person, "zip");
		            String email = commonUtils.getJSONStringProperty(person, "email");
		            String phonePrimary = commonUtils.getJSONStringProperty(person, "phonePrimary");
		            String isPrimaryPhoneMobile = commonUtils.getJSONStringProperty(person, "isPrimaryPhoneMobile");
		            String phoneSecondary = commonUtils.getJSONStringProperty(person, "phoneSecondary");
		            String isSecondaryPhoneMobile = commonUtils.getJSONStringProperty(person, "isSecondaryPhoneMobile");
		            String preferredContactMethod = commonUtils.getJSONStringProperty(person, "preferredContactMethod");
		            String taxReturn4506TRequested = commonUtils.getJSONStringProperty(person, "taxReturn4506TRequested");
		            String taxReturnFilingTypeCode = commonUtils.getJSONStringProperty(person, "taxReturnFilingTypeCode");
		            String taxReturnSpouseFirstName = commonUtils.getJSONStringProperty(person, "taxReturnSpouseFirstName");
		            String taxReturnSpouseLastName = commonUtils.getJSONStringProperty(person, "taxReturnSpouseLastName");
		            String taxReturnSpouseSSN = commonUtils.getJSONStringProperty(person, "taxReturnSpouseSSN");
		            String taxReturnFiledUnderPreviousAddress = commonUtils.getJSONStringProperty(person, "taxReturnFiledUnderPreviousAddress");
		            String taxReturnPreviousAddressStreet = commonUtils.getJSONStringProperty(person, "taxReturnPreviousAddressStreet");
		            String taxReturnPreviousAddressCity = commonUtils.getJSONStringProperty(person, "taxReturnPreviousAddressCity");
		            String taxReturnPreviousAddressStateCode = commonUtils.getJSONStringProperty(person, "taxReturnPreviousAddressStateCode");
		            String taxReturnPreviousAddressZip = commonUtils.getJSONStringProperty(person, "taxReturnPreviousAddressZip");

		            // The applicant must have an address
		            if(personType.toUpperCase().equals("APPLICANT") && address.equals("")) throw new Exception("Address is required for Applicant");

		            Long personId = personUtils.createPersonForCase("",
		            		caseResult.caseId,
							personType,
							companyName,
							firstName,
							middleName,
							lastName,
							race,
							ethnicity,
							gender,
							disability,
							SSN,
							dateOfBirth,
							preferredContactMethod,
							email,
							phonePrimary,
							isPrimaryPhoneMobile,
							phoneSecondary,
							isSecondaryPhoneMobile,
	        				address,
	        				city,
	        				state,
	        				zip);
		            
		            // Add the person's tax return
		            if(!taxReturn4506TRequested.equals("") || 
		            		!taxReturnFilingTypeCode.equals("") || 
		            		!taxReturnSpouseFirstName.equals("") || 
		            		!taxReturnSpouseSSN.equals("") || 
		            		!taxReturnFiledUnderPreviousAddress.equals("") || 
		            		!taxReturnPreviousAddressStreet.equals("") || 
		            		!taxReturnPreviousAddressCity.equals("") || 
		            		!taxReturnPreviousAddressStateCode.equals("") || 
		            		!taxReturnPreviousAddressZip.equals("")) {
		            	taxReturnUtils.createTaxReturn(submittedBy,
		            			personId,
		            			taxReturn4506TRequested,
								taxReturnFilingTypeCode,
								taxReturnSpouseFirstName,
								taxReturnSpouseLastName,
								taxReturnPreviousAddressStreet,
								taxReturnPreviousAddressCity,
								taxReturnPreviousAddressStateCode,
								taxReturnPreviousAddressZip,
								taxReturnSpouseSSN);
		            }
		    	}
		    		
		    		
		    	// *******************************************************
		    	//    Process the insurance claims
		    	// *******************************************************
		    	
		    	if(jsonInput.has("insuranceClaim")) {
		    		JSONArray claims = new JSONArray(jsonInput.getJSONArray("insuranceClaim").toString());
		    		
			    	Iterator<Object> claimsIter = claims.iterator();
			    	while(claimsIter.hasNext()) {
			    		JSONObject claim = new JSONObject(claimsIter.next().toString());
	
			    		String insuranceType = commonUtils.getJSONStringProperty(claim, "insuranceType");
			    		String claimCompanyName = commonUtils.getJSONStringProperty(claim, "companyName");
			    		String companyPhone = commonUtils.getJSONStringProperty(claim, "companyPhone");
			    		String policyNumber = commonUtils.getJSONStringProperty(claim, "policyNumber");
			    		String claimNumber = commonUtils.getJSONStringProperty(claim, "claimNumber");
			    		Long amount = commonUtils.getJSONLongProperty(claim, "amount");
			    		
			    		insuranceClaimUtils.createInsuranceClaim(submittedBy,
			    				caseResult.caseId,
			    				insuranceType,
			    				claimCompanyName,
			    				companyPhone,
			    				policyNumber,
			    				claimNumber,
			    				amount);
			    	}
		            	
		    	}
    		}
	    	
    		jsonResultObj.put("success", "true");
    		jsonResultObj.put("caseId", caseResult.caseId);
    		jsonResultObj.put("caseTextId", caseResult.caseTextId);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
	
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.getCases", mode = READ)
	public Stream<Output> getCases(@Name("submittedBy") String submittedBy,
			@Name(value="applicantId") Long applicantId,
			@Name(value="caseStatus") String caseStatus,
			@Name(value="caseMgrId") Long caseMgrId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_CASES_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ApplicantId", applicantId);
			params.put("CaseStatus", caseStatus);
			params.put("CaseMgrId", caseMgrId);

			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();

		    	JSONObject json = new JSONObject();
				Node programNode = commonUtils.checkForNullNode(db, row.get("ProgramId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("PhaseId"));
				Node caseNode = commonUtils.checkForNullNode(db, row.get("CaseId"));
				Node applicantNode = commonUtils.checkForNullNode(db, row.get("ApplicantId"));
				Node caseManagerNode = commonUtils.checkForNullNode(db, row.get("CaseMgrId"));
				Node propertyNode = commonUtils.checkForNullNode(db, row.get("PropertyId"));

		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("programName", commonUtils.getProperty(programNode, "name"));
		    	json.put("phaseName", commonUtils.getProperty(phaseNode, "name"));
		    	json.put("caseId", commonUtils.getProperty(caseNode, "id"));
		    	json.put("caseStatus", commonUtils.getProperty(programNode, "Status"));
		    	json.put("applicantId", commonUtils.getProperty(applicantNode, "id"));
		    	json.put("applicantFirstName", commonUtils.getProperty(applicantNode, "FirstName"));
		    	json.put("applicantLastName", commonUtils.getProperty(applicantNode, "LastName"));
		    	json.put("applicantLegalEntityName", commonUtils.getProperty(applicantNode, "LegalEntityName"));
		    	json.put("caseManagerId", commonUtils.getProperty(caseManagerNode, "id"));
		    	json.put("caseManagerFirstName", commonUtils.getProperty(caseManagerNode, "FirstName"));
		    	json.put("caseManagerLastName", commonUtils.getProperty(caseManagerNode, "LastName"));
		    	json.put("propertyId", commonUtils.getProperty(propertyNode, "id"));
				
		    	jsonResultArr.put(json);
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.getCaseByCaseId", mode = READ)
	public Stream<Output> getCaseByCaseId(@Name("submittedBy") String submittedBy,
			@Name("caseId") String caseId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_CASE_BY_CASE_ID_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);

			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();

		    	JSONObject json = new JSONObject();
				Node programNode = commonUtils.checkForNullNode(db, row.get("ProgramId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("PhaseId"));
				Node caseNode = commonUtils.checkForNullNode(db, row.get("CaseId"));

		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("programName", commonUtils.getProperty(programNode, "name"));
		    	json.put("phaseId", commonUtils.getProperty(phaseNode, "id"));
		    	json.put("phaseName", commonUtils.getProperty(phaseNode, "name"));
		    	json.put("caseId", commonUtils.getProperty(caseNode, "id"));
		    	json.put("caseStatus", commonUtils.getProperty(caseNode, "Status"));
		    	json.put("caseTextId", commonUtils.getProperty(caseNode, "CaseID"));
		    	json.put("caseVOAD", commonUtils.getProperty(caseNode, "VOAD"));
		    	json.put("caseReferral", commonUtils.getProperty(caseNode, "Referral"));
		    	json.put("caseAppianProcessId", commonUtils.getProperty(caseNode, "AppianProcessId"));
				
		    	jsonResultArr.put(json);
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.getPropertyByCaseId", mode = READ)
	public Stream<Output> getPropertyByCaseId(@Name("submittedBy") String submittedBy,
			@Name("caseId") String caseId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_PROPERTY_BY_CASE_ID_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);

			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();

		    	JSONObject json = new JSONObject();
				Node propertyNode = commonUtils.checkForNullNode(db, row.get("PropertyId"));
				Node addressNode = commonUtils.checkForNullNode(db, row.get("AddressId"));
				Node cityNode = commonUtils.checkForNullNode(db, row.get("CityId"));
				Node stateNode = commonUtils.checkForNullNode(db, row.get("StateId"));
				Node zipNode = commonUtils.checkForNullNode(db, row.get("ZipId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("CountyNode"));
				Node countryNode = commonUtils.checkForNullNode(db, row.get("CountryNode"));

		    	json.put("propertyId", commonUtils.getProperty(propertyNode, "id"));
		    	json.put("propertyType", commonUtils.getProperty(propertyNode, "Type"));
		    	json.put("ownershipType", commonUtils.getProperty(propertyNode, "OwnershipType"));
		    	json.put("street", commonUtils.getProperty(addressNode, "Street"));
		    	json.put("street1", commonUtils.getProperty(addressNode, "street1"));
		    	json.put("geoFloodPlane", commonUtils.getProperty(addressNode, "GeoFloodPlane"));
		    	json.put("geoParcel", commonUtils.getProperty(addressNode, "GeoParcel"));
		    	json.put("city", commonUtils.getProperty(cityNode, "name"));
		    	json.put("state", commonUtils.getProperty(stateNode, "name"));
		    	json.put("zipCode", commonUtils.getProperty(zipNode, "zip"));
		    	json.put("county", commonUtils.getProperty(countyNode, "name"));
		    	json.put("country", commonUtils.getProperty(countryNode, "name"));
				
		    	jsonResultArr.put(json);
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.getHouseholdByCaseId", mode = READ)
	public Stream<Output> getHouseholdByCaseId(@Name("submittedBy") String submittedBy,
			@Name("caseId") String caseId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_HOUSEHOLD_BY_CASE_ID_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);

			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();

		    	JSONObject json = new JSONObject();
				Node householdNode = commonUtils.checkForNullNode(db, row.get("HouseholdId"));
				Node addressNode = commonUtils.checkForNullNode(db, row.get("AddressId"));
				Node cityNode = commonUtils.checkForNullNode(db, row.get("CityId"));
				Node stateNode = commonUtils.checkForNullNode(db, row.get("StateId"));
				Node zipNode = commonUtils.checkForNullNode(db, row.get("ZipId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("CountyNode"));
				Node countryNode = commonUtils.checkForNullNode(db, row.get("CountryNode"));

		    	json.put("householdId", commonUtils.getProperty(householdNode, "id"));
		    	json.put("householdIncome", commonUtils.getProperty(householdNode, "HouseHoldIncome"));
		    	json.put("householdCount", commonUtils.getProperty(householdNode, "HouseHoldCount"));
		    	json.put("AMICategory", commonUtils.getProperty(householdNode, "AMICategory"));
		    	json.put("street", commonUtils.getProperty(addressNode, "Street"));
		    	json.put("street1", commonUtils.getProperty(addressNode, "street1"));
		    	json.put("geoFloodPlane", commonUtils.getProperty(addressNode, "GeoFloodPlane"));
		    	json.put("geoParcel", commonUtils.getProperty(addressNode, "GeoParcel"));
		    	json.put("city", commonUtils.getProperty(cityNode, "name"));
		    	json.put("state", commonUtils.getProperty(stateNode, "name"));
		    	json.put("zipCode", commonUtils.getProperty(zipNode, "zip"));
		    	json.put("county", commonUtils.getProperty(countyNode, "name"));
		    	json.put("country", commonUtils.getProperty(countryNode, "name"));
				
		    	jsonResultArr.put(json);
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.updateCase", mode = WRITE)
	public Stream<Output> updateCase(@Name("submittedBy") String submittedBy,
			@Name("caseId") long caseId,
			@Name("programId") long programId,
			@Name("phaseId") long phaseId,
			@Name("programName") String programName,
			@Name("phaseName") String phaseName,
			@Name("status") String status,
			@Name("voad") String voad,
			@Name("referral") String referral,
			@Name("appianProcessId") String appianProcessId,
			@Name("propertyType") String propertyType,
			@Name("propertyOwnershipType") String propertyOwnershipType,
			@Name("propertyStreet") String propertyStreet,
			@Name("propertyStreet1") String propertyStreet1,
			@Name("propertyCity") String propertyCity,
			@Name("propertyCounty") String propertyCounty,
			@Name("propertyState") String propertyState,
			@Name("propertyZIP") String propertyZip,
			@Name("propertyCountry") String propertyCountry,
			@Name("householdIncome") String householdIncome,
			@Name("householdCount") String householdCount,
			@Name("AMICategory") String AMICategory,
			@Name("householdStreet") String householdStreet,
			@Name("householdStreet1") String householdStreet1,
			@Name("householdCity") String householdCity,
			@Name("householdCounty") String householdCounty,
			@Name("householdState") String householdState,
			@Name("householdZip") String householdZip,
			@Name("householdCountry") String householdCountry,
			@Name("propertyGeoFloodPlane") String propertyGeoFloodPlane,
			@Name("propertyGeoParcel") String propertyGeoParcel,
			@Name("householdGeoFloodPlane") String householdGeoFloodPlane,
			@Name("householdGeoParcel") String householdGeoParcel) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);

			// Delete the existing relationships with phases
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_CASE_DEL1_QUERY");
			Result result = db.execute(cypher, params);

			// Delete the existing relationships between property addresses and zip codes
			cypher = commonUtils.getCypherQuery(db, "UPDATE_CASE_DEL2_QUERY");
			result = db.execute(cypher, params);

			// Delete the existing relationships between property addresses and cities
			cypher = commonUtils.getCypherQuery(db, "UPDATE_CASE_DEL3_QUERY");
			result = db.execute(cypher, params);

			// Delete the existing relationships between household addresses and zip codes
			cypher = commonUtils.getCypherQuery(db, "UPDATE_CASE_DEL4_QUERY");
			result = db.execute(cypher, params);

			// Delete the existing relationships between household addresses and cities
			cypher = commonUtils.getCypherQuery(db, "UPDATE_CASE_DEL5_QUERY");
			result = db.execute(cypher, params);
		
			// Update the case
			cypher = commonUtils.getCypherQuery(db, "UPDATE_CASE_QUERY");
			
			//params.put("CaseId", caseId);  ALREADY SET IN THE HASHMAP
			params.put("ProgramId", programId);
			params.put("PhaseId", phaseId);
			params.put("ProgramName", programName);
			params.put("PhaseName", phaseName);
			params.put("Status", status);
			params.put("Voad", voad);
			params.put("Referral", referral);
			params.put("AppianProcessId", appianProcessId);
			params.put("PropertyType", propertyType);
			params.put("PropertyOwnershipType", propertyOwnershipType);
			params.put("PropertyStreet", propertyStreet);
			params.put("PropertyStreet1", propertyStreet1);
			params.put("PropertyCity", propertyCity);
			params.put("PropertyCounty", propertyCounty);
			params.put("PropertyState", propertyState);
			params.put("PropertyZip", propertyZip);
			params.put("PropertyCountry", propertyCountry);
			params.put("HouseholdIncome", householdIncome);
			params.put("HouseholdCount", householdCount);
			params.put("AMICategory", AMICategory);
			params.put("HouseholdStreet", householdStreet);
			params.put("HouseholdStreet1", householdStreet1);
			params.put("HouseholdCity", householdCity);
			params.put("HouseholdCounty", householdCounty);
			params.put("HouseholdState", householdState);
			params.put("HouseholdZip", householdZip);
			params.put("HouseholdCountry", householdCountry);
			params.put("PropertyGeoFloodPlane", propertyGeoFloodPlane);
			params.put("PropertyGeoParcel", propertyGeoParcel);
			params.put("HouseholdGeoFloodPlane", householdGeoFloodPlane);
			params.put("HouseholdGeoParcel", householdGeoParcel);

			result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();

		    	JSONObject json = new JSONObject();
				Node caseNode = commonUtils.checkForNullNode(db, row.get("CaseId"));

		    	json.put("caseId", commonUtils.getProperty(caseNode, "id"));
				
		    	jsonResultArr.put(json);
		    	
		    	break;
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.getCasePhase", mode = READ)
	public Stream<Output> getCasePhase(@Name("submittedBy") String submittedBy,
			@Name(value="caseTextId") String caseTextId,
			@Name(value="phaseId") long phaseId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_CASEPHASE_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseTextId", caseTextId);
			params.put("PhaseId",  phaseId);

			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
				Node caseNode = commonUtils.checkForNullNode(db, row.get("CaseId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("PhaseId"));
				Node casePhaseNode = commonUtils.checkForNullNode(db, row.get("CasePhaseId"));
				
		    	json.put("caseId", commonUtils.getProperty(caseNode, "id"));
		    	json.put("caseTextId", commonUtils.getProperty(caseNode, "CaseID"));
		    	json.put("phaseId", commonUtils.getProperty(phaseNode, "id"));
		    	json.put("phaseStatus", commonUtils.getProperty(phaseNode, "Status"));
		    	json.put("casePhaseId", commonUtils.getProperty(casePhaseNode, "id"));
		    	json.put("casePhaseStatus", commonUtils.getProperty(casePhaseNode, "Status"));
				
		    	jsonResultArr.put(json);
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

	@Procedure(name = "horne.cdbg.sp.updateCasePhase", mode = WRITE)
	public Stream<Output> updateCasePhase(@Name("submittedBy") String submittedBy,
			@Name("caseTextId") String caseTextId,
			@Name("phaseId") long phaseId,
			@Name("casePhaseId") long casePhaseId,
			@Name("casePhaseStatus") String casePhaseStatus) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_CASEPHASE_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseTextId",  caseTextId);
			params.put("PhaseId",  phaseId);
			params.put("CasePhaseId",  casePhaseId);
			params.put("CasePhaseStatus",  casePhaseStatus);

			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();

		    	JSONObject json = new JSONObject();
		    	json.put("casePhaseId", commonUtils.getRowValueLong(row, "CasePhaseId"));
				
		    	jsonResultArr.put(json);
			}

			jsonResultObj.put("success", "true");
			jsonResultObj.put("result", jsonResultArr);
			
			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", "");
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		log.error(ex.getMessage());

			jsonErrorObj.put("code", "");
			jsonErrorObj.put("message", ex.getMessage());
			jsonErrorArr.put(jsonErrorObj);
			jsonResultObj.put("error", jsonErrorArr);
		}
		
		Output output = new Output();
		output.result = jsonResultObj.toString();
		
		stream.add(output);
	
		return stream.stream();
	}

}
