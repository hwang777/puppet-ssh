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


public class Tasks {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }
    
	@Procedure(name = "horne.cdbg.sp.getTasks", mode = READ)
	public Stream<Output> getTasks(@Name("submittedBy") String submittedBy,
			@Name("appianTaskAssignee") String appianTaskAssignee,
			@Name("status") String status,
			@Name("appianTaskId") String appianTaskId,
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
			String cypher = commonUtils.getCypherQuery(db, "GET_TASKS_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("AppianTaskAssignee", appianTaskAssignee);
			params.put("Status", status);
			params.put("AppianTaskId", appianTaskId);
			params.put("CasePhaseId", casePhaseId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);

			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
		
				Node programNode = commonUtils.checkForNullNode(db, row.get("programId"));
				Node caseNode = commonUtils.checkForNullNode(db, row.get("caseId"));
				Node phaseNode = commonUtils.checkForNullNode(db, row.get("phaseId"));
				Node taskNode = commonUtils.checkForNullNode(db, row.get("taskId"));
				Node cityNode = commonUtils.checkForNullNode(db, row.get("cityId"));
				Node countyNode = commonUtils.checkForNullNode(db, row.get("countyId"));
				Node applicantNode = commonUtils.checkForNullNode(db, row.get("applicantId"));
				Node caseMgrNode = commonUtils.checkForNullNode(db, row.get("caseMgrId"));
				Node casePhaseNode = commonUtils.checkForNullNode(db, row.get("casePhaseId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("programId", commonUtils.getProperty(programNode, "id"));
				json.put("programName", commonUtils.getProperty(programNode, "name"));
				json.put("caseId", commonUtils.getProperty(caseNode, "id"));
				json.put("caseTextId", commonUtils.getProperty(caseNode, "CaseID"));
				json.put("caseStatus", commonUtils.getProperty(caseNode, "Status"));
				json.put("programPhaseId", commonUtils.getProperty(phaseNode, "id"));
				json.put("phaseName", commonUtils.getProperty(phaseNode, "name"));
				json.put("taskId", commonUtils.getProperty(taskNode, "id"));
				json.put("taskName", commonUtils.getProperty(taskNode, "Name"));
				json.put("appianTaskStatus", commonUtils.getProperty(taskNode, "AppianTaskStatus"));
				json.put("appianTaskId", commonUtils.getProperty(taskNode, "AppianTaskId"));
				json.put("appianTaskAssignee", commonUtils.getProperty(taskNode, "AppianTaskAssignee"));
				json.put("cityId", commonUtils.getProperty(cityNode, "id"));
				json.put("cityName", commonUtils.getProperty(cityNode, "name"));
				json.put("countyId", commonUtils.getProperty(countyNode, "id"));
				json.put("countyName", commonUtils.getProperty(countyNode, "name"));
				json.put("applicantId", commonUtils.getProperty(applicantNode, "id"));
				json.put("applicantFirstName", commonUtils.getProperty(applicantNode, "FirstName"));
				json.put("applicantLastName", commonUtils.getProperty(applicantNode, "LastName"));
				json.put("caseMgrId", commonUtils.getProperty(caseMgrNode, "id"));
				json.put("caseManagerFirstName", commonUtils.getProperty(caseMgrNode, "FirstName"));
				json.put("caseManagerLastName", commonUtils.getProperty(caseMgrNode, "LastName"));
				json.put("casePhaseId", commonUtils.getProperty(casePhaseNode, "id"));
				json.put("casePhaseName", commonUtils.getProperty(casePhaseNode, "Name"));
			
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
	
	@Procedure(name = "horne.cdbg.sp.updateTask", mode = WRITE)
	public Stream<Output> updateTask(@Name("submittedBy") String submittedBy,
			@Name("casePhaseId") long casePhaseId,
			@Name("appianTaskId") String appianTaskId,
			@Name("taskName") String taskName,
			@Name("taskStatus") String taskStatus,
			@Name("appianTaskAssignee") String appianTaskAssignee,
			@Name("taskCity") String taskCity,
			@Name("taskCounty") String taskCounty) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_TASK_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CasePhaseId", casePhaseId);
			params.put("AppianTaskId", appianTaskId);
			params.put("TaskName", taskName);
			params.put("AppianTaskStatus", taskStatus);
			params.put("AppianTaskAssignee", appianTaskAssignee);
			params.put("TaskCity", taskCity);
			params.put("TaskCounty", taskCounty);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error updating the tax return");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("taskId", row.get("TaskId"));
				
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
