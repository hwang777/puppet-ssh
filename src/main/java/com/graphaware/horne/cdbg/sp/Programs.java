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

public class Programs {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }
    

	@Procedure(name = "horne.cdbg.sp.getProgram", mode = READ)
	public Stream<Output> getProgram(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_PROGRAM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
				Node eventNode = commonUtils.checkForNullNode(db, row.get("eventId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("programName", commonUtils.getProperty(programNode, "name"));
		    	json.put("programType", commonUtils.getProperty(programNode, "Type"));
		    	json.put("programState", commonUtils.getProperty(programNode, "Stats"));
		    	json.put("eligibleCensusTract", commonUtils.getProperty(programNode, "EligibleCensusTract"));
		    	json.put("eventId", commonUtils.getProperty(eventNode, "id"));
		    	json.put("eventName", commonUtils.getProperty(eventNode, "name"));
		    	json.put("eventType", commonUtils.getProperty(eventNode, "Type"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.getCountyByProgram", mode = READ)
	public Stream<Output> getCountyByProgram(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_COUNTY_BY_PROGRAM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("countyId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("countyId", commonUtils.getProperty(countyNode, "id"));
		    	json.put("countyName", commonUtils.getProperty(countyNode, "name"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.getCityByProgram", mode = READ)
	public Stream<Output> getCityByProgram(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_CITY_BY_PROGRAM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
				Node cityNode = commonUtils.checkForNullNode(db, row.get("cityId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("cityId", commonUtils.getProperty(cityNode, "id"));
		    	json.put("cityName", commonUtils.getProperty(cityNode, "name"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.getPhaseByProgram", mode = READ)
	public Stream<Output> getPhaseByProgram(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_PHASE_BY_PROGRAM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("phaseId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("programName", commonUtils.getProperty(programNode, "name"));
		    	json.put("phaseId", commonUtils.getProperty(phaseNode, "id"));
		    	json.put("phaseName", commonUtils.getProperty(phaseNode, "name"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.updateProgram", mode = WRITE)
	public Stream<Output> updateProgram(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId,
			@Name("eventName") String eventName,
			@Name("agencyName") String agencyName,
			@Name("programName") String programName,
			@Name("programType") String programType,
			@Name("programState") String programState,
			@Name("eligibleCensusTract") String eligibleCensusTract) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PROGRAM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
			params.put("EventName", eventName);
			params.put("AgencyName", agencyName);
			params.put("ProgramName", programName);
			params.put("ProgramType", programType);
			params.put("ProgramState", programState);
			params.put("EligibleCensusTract", eligibleCensusTract);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.updateProgramCounty", mode = WRITE)
	public Stream<Output> updateProgramCounty(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId,
			@Name("countyId") long countyId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PROGRAM_COUNTY_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
			params.put("CountyId", countyId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("ProgramId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("CountyId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("countyId", commonUtils.getProperty(countyNode, "id"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.updateProgramCity", mode = WRITE)
	public Stream<Output> updateProgramCity(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId,
			@Name("cityId") long cityId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PROGRAM_CITY_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
			params.put("CityId", cityId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error updating the program");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("ProgramId", row.get("ProgramId"));
		    	json.put("CityId", row.get("CityId"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.updateProgramPhase", mode = WRITE)
	public Stream<Output> updateProgramPhase(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId,
			@Name("phaseId") long phaseId,
			@Name("phaseName") String phaseName) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PROGRAM_PHASE_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
			params.put("PhaseId", phaseId);
			params.put("PhaseName", phaseName);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error updating the program");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("ProgramId", row.get("ProgramId"));
		    	json.put("PhaseId", row.get("PhaseId"));
				
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
