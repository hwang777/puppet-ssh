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

public class InsuranceClaims {
	
	public InsuranceClaims(GraphDatabaseService db, Log log) {
		super();
		this.db = db;
		this.log = log;
	}
	
	public InsuranceClaims() {
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
    

	@Procedure(name = "horne.cdbg.sp.getInsuranceClaimByCaseId", mode = READ)
	public Stream<Output> getInsuranceClaimByCaseId(@Name("submittedBy") String submittedBy,
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
			String cypher = commonUtils.getCypherQuery(db, "GET_INSURANCE_CLAIM_BY_CASE_ID_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node insuranceClaimNode = commonUtils.checkForNullNode(db, row.get("InsuranceClaimId"));

		    	JSONObject json = new JSONObject();
		    	json.put("insuranceClaimId", commonUtils.getProperty(insuranceClaimNode, "id"));
		    	json.put("insuranceClaimType", commonUtils.getProperty(insuranceClaimNode, "Type"));
		    	json.put("provider", commonUtils.getProperty(insuranceClaimNode, "Provider"));
		    	json.put("policyNumber", commonUtils.getProperty(insuranceClaimNode, "PolicyNumber"));
		    	json.put("claimNumber", commonUtils.getProperty(insuranceClaimNode, "ClaimNumber"));
		    	json.put("amount", commonUtils.getProperty(insuranceClaimNode, "Amount"));
		    	json.put("verificationStatus", commonUtils.getProperty(insuranceClaimNode, "VerificationStatus"));
		    	json.put("phoneNumber", commonUtils.getProperty(insuranceClaimNode, "Phone"));
				
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

	@Procedure(name = "horne.cdbg.sp.updateInsuranceClaim", mode = WRITE)
	public Stream<Output> updateInsuranceClaim(@Name("submittedBy") String submittedBy,
			@Name("caseId") long caseId,
			@Name("insuranceClaimId") long insuranceClaimId,
			@Name("type") String type,
			@Name("provider") String provider,
			@Name("phoneNumber") String phoneNumber,
			@Name("policyNumber") String policyNumber,
			@Name("claimNumber") String claimNumber,
			@Name("amount") String amount,
			@Name("verificationStatus") String verificationStatus) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_INSURANCE_CLAIM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseId", caseId);
			params.put("InsuranceClaimId",  insuranceClaimId);
			params.put("Type", type);
			params.put("Provider", provider);
			params.put("Phone", phoneNumber);
			params.put("PolicyNumber", policyNumber);
			params.put("ClaimNumber", claimNumber);
			params.put("Amount", amount);
			params.put("VerificationStatus", verificationStatus);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node insuranceClaimNode = commonUtils.checkForNullNode(db, row.get("insuranceClaimId"));

		    	JSONObject json = new JSONObject();
		    	json.put("insuranceClaimId", commonUtils.getProperty(insuranceClaimNode, "id"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.createInsuranceClaim", mode = WRITE)
	public Stream<Output> createInsuranceClaim(@Name("submittedBy") String submittedBy,
			@Name("caseId") Long caseId,
			@Name("type") String type,
			@Name("provider") String provider,
			@Name("phone") String phone,
			@Name("policyNumber") String policyNumber,
			@Name("claimNumber") String claimNumber,
			@Name("amount") Long amount) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_INSURANCE_CLAIM_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("caseId", caseId);
			params.put("type", type);
			params.put("provider", provider);
			params.put("phone", phone);
			params.put("policyNumber", policyNumber);
			params.put("claimNumber", claimNumber);
			params.put("amount", amount);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error creating the insurance claim");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("insuranceClaimId", row.get("insuranceClaimId"));
				
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
