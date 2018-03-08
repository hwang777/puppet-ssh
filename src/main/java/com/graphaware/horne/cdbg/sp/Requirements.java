package com.graphaware.horne.cdbg.sp;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
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

public class Requirements {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }
    

	@Procedure(name = "horne.cdbg.sp.createRequirement", mode = WRITE)
	public Stream<Output> createRequirement(@Name("submittedBy") String submittedBy) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_REQUIREMENT_QUERY");
				
			/* Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("x", x);
			*/
	
			// Run the cypher query
			Result result = db.execute(cypher);
			if(!result.hasNext()) throw new Exception("There was an error creating the requirement");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("requirementId", row.get("requirementId"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.getRequirementByProgram", mode = READ)
	public Stream<Output> getRequirementByProgram(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId,
			@Name("phaseId") long phaseId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_REQUIREMENT_BY_PROGRAM_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
			params.put("PhaseId", phaseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("phaseId"));
				Node requirementNode = commonUtils.checkForNullNode(db, row.get("requirementId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
		    	json.put("programName", commonUtils.getProperty(programNode, "name"));
		    	json.put("phaseId", commonUtils.getProperty(phaseNode, "id"));
		    	json.put("phaseName", commonUtils.getProperty(phaseNode, "name"));
		    	json.put("requirementId", commonUtils.getProperty(requirementNode, "id"));
		    	json.put("requirementName", commonUtils.getProperty(requirementNode, "name"));
		    	json.put("requirementType", commonUtils.getProperty(requirementNode, "Type"));
		    	json.put("requirementMapping", commonUtils.getProperty(requirementNode, "Mapping"));
		    	json.put("comparisonOperator", commonUtils.getProperty(requirementNode, "comparisonOperator"));
		    	json.put("comparisonValue", commonUtils.getProperty(requirementNode, "comparisonValue"));
				
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
	
	@Procedure(name = "horne.cdbg.sp.updateProgramRequirement", mode = WRITE)
	public Stream<Output> updateProgramRequirement(@Name("submittedBy") String submittedBy,
			@Name("programId") long programId,
			@Name("phaseId") long phaseId,
			@Name("requirementId") long requirementId,
			@Name("requirementName") String requirementName,
			@Name("requirementType") String requirementType,
			@Name("requirementMapping") String requirementMapping,
			@Name("comparisonOperator") String comparisonOperator,
			@Name("comparisonValue") String comparisonValue) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_PROGRAM_REQUIREMENT_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("ProgramId", programId);
			params.put("PhaseId", phaseId);
			params.put("RequirementId", requirementId);
			params.put("RequirementName", requirementName);
			params.put("RequirementType", requirementType);
			params.put("RequirementMapping", requirementMapping);
			params.put("ComparisonOperator", comparisonOperator);
			params.put("ComparisonValue", comparisonValue);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("requirementId", row.get("requirementId"));
				
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

	@Procedure(name = "horne.cdbg.sp.getRequirementDataByCasePhase", mode = READ)
	public Stream<Output> getRequirementDataByCasePhase(@Name("submittedBy") String submittedBy,
			@Name("casePhaseId") long casePhaseId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_REQUIREMENT_DATA_BY_CASE_PHASE_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CasePhaseId",  casePhaseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node programNode = commonUtils.checkForNullNode(db, row.get("ProgramId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("PhaseId"));
				Node requirementNode = commonUtils.checkForNullNode(db, row.get("RequirementId"));
				Node caseNode = commonUtils.checkForNullNode(db, row.get("CaseId"));
				Node casePhaseNode = commonUtils.checkForNullNode(db, row.get("CasePhaseId"));
				Node requirementDataNode = commonUtils.checkForNullNode(db, row.get("RequirementDataId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
				json.put("phaseId", commonUtils.getProperty(phaseNode, "id"));
				json.put("requirementId", commonUtils.getProperty(requirementNode, "id"));
				json.put("caseId", commonUtils.getProperty(caseNode, "id"));
				json.put("caseTextId", commonUtils.getProperty(caseNode, "CaseID"));
				json.put("casePhaseId", commonUtils.getProperty(casePhaseNode, "id"));
				json.put("requirementDataId", commonUtils.getProperty(requirementDataNode, "id"));
				json.put("requirementDataValue", commonUtils.getProperty(requirementDataNode, "Value"));
				json.put("requirementDataStatus", commonUtils.getProperty(requirementDataNode, "Status"));
		    	
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

	@Procedure(name = "horne.cdbg.sp.createRequirementData", mode = WRITE)
	public Stream<Output> createRequirementData(@Name("submittedBy") String submittedBy) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_REQUIREMENT_DATA_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			//params.put("CasePhaseId",  casePhaseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("requirementDataId", commonUtils.getRowValueLong(row, "RequirementDataId"));
		    	
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

	@Procedure(name = "horne.cdbg.sp.updateRequirementData", mode = WRITE)
	public Stream<Output> updateRequirementData(@Name("submittedBy") String submittedBy,
			@Name("requirementId") long requirementId,
			@Name("casePhaseId") long casePhaseId,
			@Name("requirementDataId") long requirementDataId,
			@Name("requirementDataValue") String requirementDataValue,
			@Name("requirementDataStatus") String requirementDataStatus) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
		JSONArray  jsonResultArr = new JSONArray();
		
		JSONObject jsonErrorObj = new JSONObject();
		JSONArray  jsonErrorArr = new JSONArray();
		
		Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_REQUIREMENT_DATA_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("RequirementId",  requirementId);
			params.put("CasePhaseId",  casePhaseId);
			params.put("RequirementDataId",  requirementDataId);
			params.put("RequirementDataValue",  requirementDataValue);
			params.put("RequirementDataStatus",  requirementDataStatus);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("requirementDataId", commonUtils.getRowValueLong(row, "RequirementDataId"));
		    	
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
