package com.graphaware.horne.cdbg.sp;

import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.ArrayList;
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

public class Persons {
	
	public Persons(GraphDatabaseService db, Log log) {
		super();
		this.db = db;
		this.log = log;
	}
	
	public Persons() {
		super();
	}

	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }

    
	/*
	 *  Creates a Person and adds it to the case with the specified relationship
	 *  	HAS_APPLICANT   -> Applicant
	 *  	HAS_COAPPLICANT -> Co-Applicant
	 *  	HAS_HOUSEHOLD_M -> Household Member
	 *  	HAS_DESIGNEE    -> Communication Designee
	 *  	HAS_POWEROFATTY -> Power of Attorney
	 *  
	 *  The following people are associated through a Property
	 *  	HAS_TENANT      -> Tenant
	 *  	HAS_LANDOWNER   -> Land Owner
	 *  	HAS_LIENHOLDER  -> Lien Holder
	 */
    @Procedure(name = "horne.cdbg.sp.createPerson", mode = WRITE)
	public Stream<Output> createPerson(@Name("submittedBy") String submittedBy,
			@Name("caseId") Long caseId,
			@Name("personType") String personType,
			@Name("legalEntityName") String legalEntityName,
			@Name("firstName") String firstName,
			@Name("middleName") String middleName,
			@Name("lastName") String lastName,
			@Name("race") String race,
			@Name("ethnicity") String ethnicity,
			@Name("gender") String gender,
			@Name("disability") String disability,
			@Name("ssn") String ssn,
			@Name("dob") String dob,
			@Name("preferredContactMethod") String preferredContactMethod,
			@Name(value="email", defaultValue="") String email,
			@Name(value="phonePrimary", defaultValue="") String phonePrimary,
			@Name(value="isPrimaryPhoneMobile", defaultValue="") String isPrimaryPhoneMobile,
			@Name(value="phoneSecondary", defaultValue="") String phoneSecondary,
			@Name(value="isSecondaryPhoneMobile", defaultValue="") String isSecondaryPhoneMobile,
			@Name(value="address", defaultValue="") String address,
			@Name(value="city", defaultValue="") String city,
			@Name(value="state", defaultValue="") String state,
			@Name(value="zip", defaultValue="") String zip) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
			Long personId = createPersonForCase("",
					caseId,
					personType,
					legalEntityName,
					firstName,
					middleName,
					lastName,
					race,
					ethnicity,
					gender,
					disability,
					ssn,
					dob,
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

			JSONObject json = new JSONObject();
		    json.put("personId", personId);
		    jsonResultArr.put(json);            

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
    
    /*
     *  Creates a Person and adds it to the case with the specified relationship
     *      HAS_APPLICANT   -> Applicant
     *      HAS_COAPPLICANT -> Co-Applicant
     *      HAS_HOUSEHOLD_M -> Household Member
     *      HAS_DESIGNEE    -> Communication Designee
     *      HAS_POWEROFATTY -> Power of Attorney
     *  
     *  The following people are associated through a Property
     *      HAS_TENANT      -> Tenant
     *      HAS_LANDOWNER   -> Land Owner
     *      HAS_LIENHOLDER  -> Lien Holder
     */
    public Long createPersonForCase(String submittedBy,
    		Long caseId,
            String personType,
            String legalEntityName,
            String firstName,
            String middleName,
            String lastName,
            String race,
            String ethnicity,
            String gender,
            String disability,
            String ssn,
            String dob,
            String preferredContactMethod,
            String email,
            String phonePrimary,
            String isPrimaryPhoneMobile,
            String phoneSecondary,
            String isSecondaryPhoneMobile,
            String address,
            String city,
            String state,
            String zip) throws Throwable
    {          	
        Long personId = new Long(0);
        String cypher = "";
         
        // Translate the incoming personType to a named relationship
        switch(personType.toUpperCase()) {
        case "APPLICANT":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_APPLICANT_QUERY");
            break;
        case "CO-APPLICANT":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_COAPPLICANT_QUERY");
            break;
        case "HOUSEHOLD MEMBER":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_HOUSEHOLD_MEMBER_QUERY");
            break;
        case "COMMUNICATION DESIGNEE":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_DESIGNEE_QUERY");
            break;
        case "POWER OF ATTORNEY":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_POWER_OF_ATTORNEY_QUERY");
            break;
        case "TENANT":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_TENANT_QUERY");
            break;
        case "LAND OWNER":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_LANDOWNER_QUERY");
            break;
        case "LIEN HOLDER":
            cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_LIENHOLDER_QUERY");
            break;
        default:
            throw new Exception("Unknown personType: " + personType);
        }
         
        // Set up the parameters
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("caseId", caseId);
        params.put("legalEntityName", legalEntityName);
        params.put("firstName", firstName);
        params.put("middleName", middleName);
        params.put("lastName", lastName);
        params.put("race", race);
        params.put("ethnicity", ethnicity);
        params.put("gender", gender);
        params.put("disability", disability);
        params.put("ssn", ssn);
        params.put("dob", dob);
        params.put("preferredContactMethod", preferredContactMethod);

        Result result = db.execute(cypher, params);
        if(!result.hasNext()) throw new Exception("There was an error creating the person");

        while(result.hasNext())
        {
            Map<String, Object> row = result.next();
            
            personId = commonUtils.getRowValueLong(row, "personId");
            break;
        }
        
        
		// **********************************************
        //    Add the person's address
		// **********************************************
        if(!address.equals("")) {
        	if(city.equals("")) throw new Exception("Missing city for person: " + firstName + " " + lastName);
        	if(state.equals("")) throw new Exception("Missing state for person: " + firstName + " " + lastName);
        	if(zip.equals("")) throw new Exception("Missing zip for person: " + firstName + " " + lastName);
       		
    		// Validate the state
			params = new HashMap<String, Object>();
			params.put("state", state);
			result = db.execute("MATCH (state:State{name:{state}}) RETURN id(state) AS stateId", params);
    		if(!result.hasNext()) throw new Exception("The state was not found for person: " + firstName + " " + lastName);
		
    		// Add the address
    		createAddress("",
    				personId,
    				address,
    				city,
    				state,
    				zip);
        }
		// *************Address
        		
        
		// **********************************************
        //    Add the person's contact details
		// **********************************************
        if(!email.equals("")) {
    		cypher = commonUtils.getCypherQuery(db, "CREATE_CONTACT_EMAIL");
			params = new HashMap<String, Object>();
			params.put("personId", personId);
			params.put("email", email);
			db.execute(cypher, params);
        }
        
        if(!phonePrimary.equals("")) {
        	if(isPrimaryPhoneMobile.toUpperCase().equals("TRUE")) {
        		cypher = commonUtils.getCypherQuery(db, "CREATE_CONTACT_MOBILE");
    			params = new HashMap<String, Object>();
    			params.put("personId", personId);
    			params.put("mobile", phonePrimary);
    			db.execute(cypher, params);
        	} else {
        		cypher = commonUtils.getCypherQuery(db, "CREATE_CONTACT_PHONE");
    			params = new HashMap<String, Object>();
    			params.put("personId", personId);
    			params.put("phone", phonePrimary);
    			db.execute(cypher, params);
        	}
        }
        
        if(!phoneSecondary.equals("")) {
        	if(isSecondaryPhoneMobile.toUpperCase().equals("TRUE")) {
        		cypher = commonUtils.getCypherQuery(db, "CREATE_CONTACT_MOBILE");
    			params = new HashMap<String, Object>();
    			params.put("personId", personId);
    			params.put("mobile", phoneSecondary);
    			db.execute(cypher, params);
        	} else {
        		cypher = commonUtils.getCypherQuery(db, "CREATE_CONTACT_PHONE");
    			params = new HashMap<String, Object>();
    			params.put("personId", personId);
    			params.put("phone", phoneSecondary);
    			db.execute(cypher, params);
        	}
        }
		// *************Contact details
     
        return personId;
    } 
    
    /*
     *  Adds an Address to a Person
     */
    private Long createAddress(String submittedBy,
    		Long personId,
            String street,
            String city,
            String state,
            String zip) throws Throwable
    {
        Long addressId = new Long(0);
         
        // Get the cypher query
        String cypher = commonUtils.getCypherQuery(db, "CREATE_ADDRESS_QUERY");
             
        // Set up the parameters
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("personId", personId);
        params.put("street", street);
        params.put("city", city);
        params.put("state", state);
        params.put("zip", zip);
 
        Result result = db.execute(cypher, params);
        if(!result.hasNext()) throw new Exception("There was an error creating the address");
 
        while(result.hasNext())
        {
            Map<String, Object> row = result.next();
            
            addressId = commonUtils.getRowValueLong(row, "addressId");
            break;
        }
     
        return addressId;
    }

	@Procedure(name = "horne.cdbg.sp.getPersonByCaseId", mode = READ)
	public Stream<Output> getPersonByCaseId(@Name("submittedBy") String submittedBy,
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
			String cypher = commonUtils.getCypherQuery(db, "GET_PERSON_BY_CASE_ID_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				String relationshipType = row.get("RelationshipType").toString();
				Node personNode = commonUtils.checkForNullNode(db, row.get("PersonId"));
				Node piiNode = commonUtils.checkForNullNode(db, row.get("PIIId"));
				Node addressNode = commonUtils.checkForNullNode(db, row.get("AddressId"));
				Node cityNode = commonUtils.checkForNullNode(db, row.get("CityId"));
				Node stateNode = commonUtils.checkForNullNode(db, row.get("StateId"));
				Node zipNode = commonUtils.checkForNullNode(db, row.get("ZipId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("CountyId"));
				Node countryNode = commonUtils.checkForNullNode(db, row.get("CountryId"));
				Node phoneNode = commonUtils.checkForNullNode(db, row.get("PhoneId"));
				Node mobileNode = commonUtils.checkForNullNode(db, row.get("MobileId"));
				Node emailNode = commonUtils.checkForNullNode(db, row.get("EmailId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("personId", commonUtils.getProperty(personNode, "id"));
				json.put("relationshipType", relationshipType);
				json.put("userName", commonUtils.getProperty(personNode, "UserName"));
				json.put("legalEntityId", commonUtils.getProperty(personNode, "LegalEntityId"));
				json.put("legalEntityName", commonUtils.getProperty(personNode, "LegalEntityName"));
				json.put("firstName", commonUtils.getProperty(personNode, "FirstName"));
				json.put("middleName", commonUtils.getProperty(personNode, "MiddleName"));
				json.put("lastName", commonUtils.getProperty(personNode, "LastName"));
				json.put("race", commonUtils.getProperty(personNode, "Race"));
				json.put("gender", commonUtils.getProperty(personNode, "Gender"));
				json.put("disable", commonUtils.getProperty(personNode, "Disable"));
				json.put("totalIncome", commonUtils.getProperty(personNode, "TotalIncome"));
				json.put("preferredContactMethod", commonUtils.getProperty(personNode, "PreferredContactMethod"));
				json.put("ethnicity", commonUtils.getProperty(personNode, "Ethnicity"));
				json.put("ssn", commonUtils.getProperty(piiNode, "SSN"));
				json.put("dob", commonUtils.getProperty(piiNode, "DOB"));
				json.put("street", commonUtils.getProperty(addressNode, "street"));
				json.put("street1", commonUtils.getProperty(addressNode, "street1"));
				json.put("geoFloodPlane", commonUtils.getProperty(addressNode, "GeoFloodPlane"));
				json.put("geoParcel", commonUtils.getProperty(addressNode, "GeoParcel"));
				json.put("city", commonUtils.getProperty(cityNode, "name"));
				json.put("state", commonUtils.getProperty(stateNode, "name"));
				json.put("zipCode", commonUtils.getProperty(zipNode, "zip"));
				json.put("county", commonUtils.getProperty(countyNode, "name"));
				json.put("country", commonUtils.getProperty(countryNode, "name"));
				json.put("phoneNumber", commonUtils.getProperty(phoneNode, "Value"));
				json.put("mobileNumber", commonUtils.getProperty(mobileNode, "Value"));
				json.put("emailAddress", commonUtils.getProperty(emailNode, "Value"));
				
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

	@Procedure(name = "horne.cdbg.sp.getPersonPropertyByCaseId", mode = READ)
	public Stream<Output> getPersonPropertyByCaseId(@Name("submittedBy") String submittedBy,
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
			String cypher = commonUtils.getCypherQuery(db, "GET_PERSON_PROPERTY_BY_CASE_ID_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				String relationshipType = row.get("RelationshipType").toString();
				Node personNode = commonUtils.checkForNullNode(db, row.get("PersonId"));
				Node piiNode = commonUtils.checkForNullNode(db, row.get("PIIId"));
				Node addressNode = commonUtils.checkForNullNode(db, row.get("AddressId"));
				Node cityNode = commonUtils.checkForNullNode(db, row.get("CityId"));
				Node stateNode = commonUtils.checkForNullNode(db, row.get("StateId"));
				Node zipNode = commonUtils.checkForNullNode(db, row.get("ZipId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("CountyId"));
				Node countryNode = commonUtils.checkForNullNode(db, row.get("CountryId"));
				Node phoneNode = commonUtils.checkForNullNode(db, row.get("PhoneId"));
				Node mobileNode = commonUtils.checkForNullNode(db, row.get("MobileId"));
				Node emailNode = commonUtils.checkForNullNode(db, row.get("EmailId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("personId", commonUtils.getProperty(personNode, "id"));
				json.put("relationshipType", relationshipType);
				json.put("userName", commonUtils.getProperty(personNode, "UserName"));
				json.put("legalEntityId", commonUtils.getProperty(personNode, "LegalEntityId"));
				json.put("legalEntityName", commonUtils.getProperty(personNode, "LegalEntityName"));
				json.put("firstName", commonUtils.getProperty(personNode, "FirstName"));
				json.put("middleName", commonUtils.getProperty(personNode, "MiddleName"));
				json.put("lastName", commonUtils.getProperty(personNode, "LastName"));
				json.put("race", commonUtils.getProperty(personNode, "Race"));
				json.put("gender", commonUtils.getProperty(personNode, "Gender"));
				json.put("disable", commonUtils.getProperty(personNode, "Disable"));
				json.put("totalIncome", commonUtils.getProperty(personNode, "TotalIncome"));
				json.put("preferredContactMethod", commonUtils.getProperty(personNode, "PreferredContactMethod"));
				json.put("ethnicity", commonUtils.getProperty(personNode, "Ethnicity"));
				json.put("ssn", commonUtils.getProperty(piiNode, "SSN"));
				json.put("dob", commonUtils.getProperty(piiNode, "DOB"));
				json.put("street", commonUtils.getProperty(addressNode, "street"));
				json.put("street1", commonUtils.getProperty(addressNode, "street1"));
				json.put("geoFloodPlane", commonUtils.getProperty(addressNode, "GeoFloodPlane"));
				json.put("geoParcel", commonUtils.getProperty(addressNode, "GeoParcel"));
				json.put("city", commonUtils.getProperty(cityNode, "name"));
				json.put("state", commonUtils.getProperty(stateNode, "name"));
				json.put("zipCode", commonUtils.getProperty(zipNode, "zip"));
				json.put("county", commonUtils.getProperty(countyNode, "name"));
				json.put("country", commonUtils.getProperty(countryNode, "name"));
				json.put("phoneNumber", commonUtils.getProperty(phoneNode, "Value"));
				json.put("mobileNumber", commonUtils.getProperty(mobileNode, "Value"));
				json.put("emailAddress", commonUtils.getProperty(emailNode, "Value"));
				
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

	@Procedure(name = "horne.cdbg.sp.getPersonContactInfo", mode = READ)
	public Stream<Output> getPersonContactInfo(@Name("submittedBy") String submittedBy,
			@Name("personId") long personId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_PERSON_CONTACT_INFO_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("PersonId", personId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				String contactType = row.get("Type").toString();
				String contactValue = row.get("Value").toString();
				String isPreferredContactMethod = row.get("Preferred").toString();
	
		    	JSONObject json = new JSONObject();
		    	json.put("contactType", contactType);
		    	json.put("contactValue", contactValue);
		    	json.put("isPreferredContactMethod", isPreferredContactMethod);
				
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

	@Procedure(name = "horne.cdbg.sp.getPersonIncome", mode = READ)
	public Stream<Output> getPersonIncome(@Name("submittedBy") String submittedBy,
			@Name("personId") long personId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_PERSON_INCOME_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("PersonId", personId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node incomeNode = commonUtils.checkForNullNode(db, row.get("IncomeId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("incomeId", commonUtils.getProperty(incomeNode, "id"));
				json.put("amount", commonUtils.getProperty(incomeNode, "Ammount"));
				json.put("source", commonUtils.getProperty(incomeNode, "Source"));
				json.put("verificationStatus", commonUtils.getProperty(incomeNode, "VerificationStatus"));
				json.put("verificationDocument", commonUtils.getProperty(incomeNode, "VerificationDocument"));
				
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

	@Procedure(name = "horne.cdbg.sp.updatePerson", mode = WRITE)
	public Stream<Output> updatePerson(@Name("submittedBy") String submittedBy,
			@Name("PersonId") long personId,
			@Name("LegalEntityName") String legalEntityName,
			@Name("FirstName") String firstName,
			@Name("MiddleName") String middleName,
			@Name("LastName") String lastName,
			@Name("Race") String race,
			@Name("Ethnicity") String ethnicity,
			@Name("Gender") String gender,
			@Name("Disability") String disability,
			@Name("SSN") String ssn,
			@Name("DOB") String dob,
			@Name("Phone") String phoneNumber,
			@Name("Mobile") String mobileNumber,
			@Name("Email") String emailAddress,
			@Name("PreferredContactMethod") String preferredContactMethod,
			@Name("Street") String street,
			@Name("Street1") String street1,
			@Name("City") String city,
			@Name("County") String county,
			@Name("State") String state,
			@Name("Zip") String zipCode,
			@Name("GeoFloodPlane") String geoFloodPlane,
			@Name("GeoParcel") String geoParcel) throws Throwable
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
			params.put("PersonId", personId);

			// Delete the existing relationships between addresses and zip codes
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PERSON_DEL1_QUERY");
			Result result = db.execute(cypher, params);

			// Delete the existing relationships between addresses and cities
			cypher = commonUtils.getCypherQuery(db, "UPDATE_PERSON_DEL2_QUERY");
			result = db.execute(cypher, params);

			// Delete the existing relationships between addresses and counties
			cypher = commonUtils.getCypherQuery(db, "UPDATE_PERSON_DEL3_QUERY");
			result = db.execute(cypher, params);
			
			
			// Update the person
			//params.put("PersonId", personId);  ALREADY SET IN THE HASHMAP
			params.put("LegalEntityName", legalEntityName);
			params.put("FirstName", firstName);
			params.put("MiddleName", middleName);
			params.put("LastName", lastName);
			params.put("Race", race);
			params.put("Ethnicity", ethnicity);
			params.put("Gender", gender);
			params.put("Disability", disability);
			params.put("SSN", ssn);
			params.put("DOB", dob);
			params.put("Phone", phoneNumber);
			params.put("Mobile", mobileNumber);
			params.put("Email", emailAddress);
			params.put("PreferredContactMethod", preferredContactMethod);
			params.put("Street", street);
			params.put("Street1", street1);
			params.put("City", city);
			params.put("County", county);
			params.put("State", state);
			params.put("Zip", zipCode);
			params.put("GeoFloodPlane", geoFloodPlane);
			params.put("GeoParcel", geoParcel);

			cypher = commonUtils.getCypherQuery(db, "UPDATE_PERSON_QUERY");
			
			result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("personId", row.get("PersonId"));
				
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

	@Procedure(name = "horne.cdbg.sp.createPersonIncome", mode = WRITE)
	public Stream<Output> createPersonIncome(@Name("submittedBy") String submittedBy,
			@Name("personId") long personId,
			@Name("amount") String amount,
			@Name("source") String source,
			@Name("verificationStatus") String verificationStatus,
			@Name("verificationDocument") String verificationDocument) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_PERSON_INCOME_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("PersonId", personId);
			params.put("Amount", amount);
			params.put("Source", source);
			params.put("VerificationStatus", verificationStatus);
			params.put("VerificationDocument", verificationDocument);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node incomeNode = commonUtils.checkForNullNode(db, row.get("IncomeId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("incomeId", commonUtils.getProperty(incomeNode, "id"));
				
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

	@Procedure(name = "horne.cdbg.sp.updatePersonIncome", mode = WRITE)
	public Stream<Output> updatePersonIncome(@Name("submittedBy") String submittedBy,
			@Name("personId") long personId,
			@Name("incomeId") long incomeId,
			@Name("amount") String amount,
			@Name("source") String source,
			@Name("verificationStatus") String verificationStatus,
			@Name("verificationDocument") String verificationDocument) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PERSON_INCOME_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("PersonId", personId);
			params.put("IncomeId",  incomeId);
			params.put("Amount", amount);
			params.put("Source", source);
			params.put("VerificationStatus", verificationStatus);
			params.put("VerificationDocument", verificationDocument);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node incomeNode = commonUtils.checkForNullNode(db, row.get("IncomeId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("incomeId", commonUtils.getProperty(incomeNode, "id"));
				
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
