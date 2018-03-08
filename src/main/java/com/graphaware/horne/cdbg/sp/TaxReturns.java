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
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.WRITE;

public class TaxReturns {
	
	public TaxReturns(GraphDatabaseService db, Log log) {
		super();
		this.db = db;
		this.log = log;
	}
	
	public TaxReturns() {
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

	@Procedure(name = "horne.cdbg.sp.createTaxReturn", mode = WRITE)
	public Stream<Output> createTaxReturn(@Name("submittedBy") String submittedBy,
			@Name("personId") Long personId,
			@Name("taxReturn4506TRequested") String taxReturn4506TRequested,
			@Name("taxReturnFilingTypeCode") String taxReturnFilingTypeCode,
			@Name("taxReturnSpouseFirstName") String taxReturnSpouseFirstName,
			@Name("taxReturnSpouseLastName") String taxReturnSpouseLastName,
			@Name("previousStreet") String previousStreet,
			@Name("previousCity") String previousCity,
			@Name("previousState") String previousState,
			@Name("previousZip") String previousZip,
			@Name("spouseSSN") String spouseSSN) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_TAX_RETURN_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("personId", personId);
			params.put("taxReturn4506TRequested", taxReturn4506TRequested);
			params.put("taxReturnFilingTypeCode", taxReturnFilingTypeCode);
			params.put("taxReturnSpouseFirstName", taxReturnSpouseFirstName);
			params.put("taxReturnSpouseLastName", taxReturnSpouseLastName);
			params.put("previousStreet", previousStreet);
			params.put("previousCity", previousCity);
			params.put("previousState", previousState);
			params.put("previousZip", previousZip);
			params.put("spouseSSN", spouseSSN);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error creating the tax return");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("taxReturnId", row.get("taxReturnId"));
				
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

	@Procedure(name = "horne.cdbg.sp.updateTaxReturn", mode = WRITE)
	public Stream<Output> updateTaxReturn(@Name("submittedBy") String submittedBy,
			@Name("personId") Long personId,
			@Name("taxReturnId") Long taxReturnId,
			@Name("taxReturn4506TRequested") String taxReturn4506TRequested,
			@Name("taxReturnFilingTypeCode") String taxReturnFilingTypeCode,
			@Name("taxReturnSpouseFirstName") String taxReturnSpouseFirstName,
			@Name("taxReturnSpouseLastName") String taxReturnSpouseLastName,
			@Name("previousStreet") String previousStreet,
			@Name("previousCity") String previousCity,
			@Name("previousState") String previousState,
			@Name("previousZip") String previousZip,
			@Name("spouseSSN") String spouseSSN) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_TAX_RETURN_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("PersonId", personId);
			params.put("TaxReturnId", taxReturnId);
			params.put("TaxReturn4506TRequested", taxReturn4506TRequested);
			params.put("TaxReturnFilingTypeCode", taxReturnFilingTypeCode);
			params.put("TaxReturnSpouseFirstName", taxReturnSpouseFirstName);
			params.put("TaxReturnSpouseLastName", taxReturnSpouseLastName);
			params.put("PreviousStreet", previousStreet);
			params.put("PreviousCity", previousCity);
			params.put("PreviousState", previousState);
			params.put("PreviousZip", previousZip);
			params.put("SpouseSSN", spouseSSN);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error updating the tax return");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("taxReturnId", row.get("TaxReturnId"));
				
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
