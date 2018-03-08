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

public class Events {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }
    
	
	@Procedure(name = "horne.cdbg.sp.createEvent", mode = WRITE)
	public Stream<Output> createEvent(@Name("submittedBy") String submittedBy,
			@Name("eventName") String eventName,
			@Name("eventType") String eventType) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "CREATE_EVENT_QUERY");
				
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("eventName", eventName);
			params.put("eventType", eventType);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
			if(!result.hasNext()) throw new Exception("There was an error creating the event");
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
	
		    	JSONObject json = new JSONObject();
		    	json.put("eventId", row.get("eventId"));
				
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
