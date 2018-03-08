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

public class Communications {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }

	@Procedure(name = "horne.cdbg.sp.createCommunication", mode = WRITE)
	public Stream<Output> createCommunication(@Name("submittedBy") String submittedBy) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_COMMUNICATION_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			//params.put("name", name);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("communicationId", commonUtils.getRowValueLong(row, "CommunicationId"));
				
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

	@Procedure(name = "horne.cdbg.sp.getCommunication", mode = READ)
	public Stream<Output> getCommunication(@Name("submittedBy") String submittedBy,
			@Name("caseTextId") String caseTextId,
			@Name("dateFrom") long dateFrom,
			@Name("dateTo") long dateTo) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_COMMUNICATION_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CaseTextId",  caseTextId);
			params.put("DateFrom",  dateFrom);
			params.put("DateTo",  dateTo);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				Node caseNode = commonUtils.checkForNullNode(db, row.get("CaseId"));
				Node communicationNode = commonUtils.checkForNullNode(db, row.get("CommunicationId"));
	
		    	JSONObject json = new JSONObject();
		    	json.put("caseId", commonUtils.getProperty(caseNode, "id"));
		    	json.put("communicationId", commonUtils.getProperty(communicationNode, "id"));
				json.put("sentFrom", commonUtils.getProperty(communicationNode, "SentFrom"));
				json.put("sentTo", commonUtils.getProperty(communicationNode, "SentTo"));
				json.put("method", commonUtils.getProperty(communicationNode, "Method"));
				json.put("createdBy", commonUtils.getProperty(communicationNode, "CreatedBy"));
				json.put("docId", commonUtils.getProperty(communicationNode, "DocId"));
				json.put("description", commonUtils.getProperty(communicationNode, "Description"));
				json.put("context", commonUtils.getProperty(communicationNode, "Context"));
				json.put("content", commonUtils.getProperty(communicationNode, "Content"));

				if(communicationNode.hasProperty("CreatedTime"))
					json.put("createdTime", Long.parseLong(communicationNode.getProperty("CreatedTime").toString()));
				
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

	@Procedure(name = "horne.cdbg.sp.updateCommunication", mode = WRITE)
	public Stream<Output> updateCommunication(@Name("submittedBy") String submittedBy,
			@Name("communicationId") long communicationId,
			@Name("sentFrom") String sentFrom,
			@Name("sentTo") String sentTo,
			@Name("method") String method,
			@Name("createdBy") String createdBy,
			@Name("docId") String docId,
			@Name("description") String description,
			@Name("context") String context,
			@Name("content") String content,
			@Name("caseTextId") String caseTextId) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "UPDATE_COMMUNICATION_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("CommunicationId",  communicationId);
			params.put("SentFrom",  sentFrom);
			params.put("SentTo",  sentTo);
			params.put("Method",  method);
			params.put("CreatedBy",  createdBy);
			params.put("DocId",  docId);
			params.put("Description",  description);
			params.put("Context",  context);
			params.put("Content",  content);
			params.put("CaseTextId",  caseTextId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("communicationId", commonUtils.getRowValueLong(row, "CommunicationId"));
				
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
