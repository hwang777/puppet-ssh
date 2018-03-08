package com.graphaware.horne.cdbg.sp;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
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
import static org.neo4j.procedure.Mode.READ;

public class Questionnaires {
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	private static final CommonUtils commonUtils = new CommonUtils();
	
	
    public static class Output {
    	public String result;
    }
	
	@Procedure(name = "horne.cdbg.sp.getQuestions", mode = READ)
	public Stream<Output> getQuestions(@Name("submittedBy") String submittedBy) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Get the cypher query
			String cypher = commonUtils.getCypherQuery(db, "GET_QUESTIONS_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			//params.put("name", name);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				
				Long questionLibraryId = commonUtils.getRowValueLong(row, "questionLibraryId");
				Long sortId = commonUtils.getRowValueLong(row, "sortId");
				String question = commonUtils.getRowValueString(row, "question");
				String phase = commonUtils.getRowValueString(row, "phase");
				String category = commonUtils.getRowValueString(row, "category");
				String subCategory = commonUtils.getRowValueString(row, "subCategory");
				String questionType = commonUtils.getRowValueString(row, "questionType");
				String required = commonUtils.getRowValueString(row, "required");
				String appianMapping = commonUtils.getRowValueString(row, "appianMapping");
	
		    	JSONObject json = new JSONObject();
		    	json.put("questionLibraryId", questionLibraryId);
		    	json.put("sortId", sortId);
		    	json.put("question", question);
		    	json.put("phase", phase);
		    	json.put("category", category);
		    	json.put("subCategory", subCategory);
		    	json.put("questionType", questionType);
		    	json.put("required", required);
		    	json.put("appianMapping", appianMapping);
		    	
		    	// *******************************
		    	// Process the question options
		    	// *******************************

				// Get the cypher query
		    	String optionsCypher = commonUtils.getCypherQuery(db, "GET_QUESTION_OPTIONS_QUERY");
				
				// Set up the parameters
				Map<String, Object> optionsParams = new HashMap<String, Object>();
				optionsParams.put("questionLibraryId", questionLibraryId);
		
				// Run the cypher query
				Result optionsResult = db.execute(optionsCypher, optionsParams);
				
		    	if(optionsResult.hasNext()) {
		    		
		    		JSONArray optionsArr = new JSONArray();
			
					// Process the results
					while(optionsResult.hasNext())
					{
						Map<String, Object> optionRow = optionsResult.next();
						
						String id = commonUtils.getRowValueString(optionRow, "id");
						Long optionSortId = commonUtils.getRowValueLong(optionRow, "sortId");
						String value = commonUtils.getRowValueString(optionRow, "value");

				    	JSONObject options = new JSONObject();
				    	options.put("questionOptionLibraryId", id);
				    	options.put("sortId", optionSortId);
				    	options.put("value", value);
				    	
				    	optionsArr.put(options);				    	
					}
					
					json.put("options", optionsArr);
					
				}
		    	// ******************************* OPTIONS
				
		    	jsonResultArr.put(json);
			}
		
			jsonResultObj.put("success", "true");
			jsonResultObj.put("question", jsonResultArr);
			
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
	
	@Procedure(name = "horne.cdbg.sp.getProgramQuestions", mode = READ)
	public Stream<Output> getProgramQuestions(@Name("submittedBy") String submittedBy,
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
			String cypher = commonUtils.getCypherQuery(db, "GET_PROGRAM_QUESTIONS_QUERY");
			
			// Set up the parameters
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("programId", programId);
	
			// Run the cypher query
			Result result = db.execute(cypher, params);
	
			// Process the results
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				
				Long questionLibraryId = commonUtils.getRowValueLong(row, "questionLibraryId");
				Long sortId = commonUtils.getRowValueLong(row, "sortId");
				String question = commonUtils.getRowValueString(row, "question");
				String phase = commonUtils.getRowValueString(row, "phase");
				String category = commonUtils.getRowValueString(row, "category");
				String subCategory = commonUtils.getRowValueString(row, "subCategory");
				String questionType = commonUtils.getRowValueString(row, "questionType");
				String required = commonUtils.getRowValueString(row, "required");
				String active = commonUtils.getRowValueString(row, "active");
				String appianMapping = commonUtils.getRowValueString(row, "appianMapping");
	
		    	JSONObject json = new JSONObject();
		    	json.put("questionLibraryId", questionLibraryId);
		    	json.put("sortId", sortId);
		    	json.put("question", question);
		    	json.put("phase", phase);
		    	json.put("category", category);
		    	json.put("subCategory", subCategory);
		    	json.put("questionType", questionType);
		    	json.put("required", required);
		    	json.put("active", active);
		    	json.put("appianMapping", appianMapping);
		    	
		    	// *******************************
		    	// Process the question options
		    	// *******************************

				// Get the cypher query
		    	String optionsCypher = commonUtils.getCypherQuery(db, "GET_PROGRAM_QUESTION_OPTIONS_QUERY");
				
				// Set up the parameters
				Map<String, Object> optionsParams = new HashMap<String, Object>();
				optionsParams.put("programId", programId);
				optionsParams.put("questionLibraryId", questionLibraryId);
		
				// Run the cypher query
				Result optionsResult = db.execute(optionsCypher, optionsParams);
				
		    	if(optionsResult.hasNext()) {
		    		
		    		JSONArray optionsArr = new JSONArray();
			
					// Process the results
					while(optionsResult.hasNext())
					{
						Map<String, Object> optionRow = optionsResult.next();
						
						String id = commonUtils.getRowValueString(optionRow, "id");
						Long optionSortId = commonUtils.getRowValueLong(optionRow, "sortId");
						String value = commonUtils.getRowValueString(optionRow, "value");

				    	JSONObject options = new JSONObject();
				    	options.put("questionOptionLibraryId", id);
				    	options.put("sortId", optionSortId);
				    	options.put("value", value);
				    	
				    	optionsArr.put(options);				    	
					}
					
					json.put("options", optionsArr);
					
				}
		    	// ******************************* OPTIONS
				
		    	jsonResultArr.put(json);
			}
		
			jsonResultObj.put("success", "true");
			jsonResultObj.put("programId", programId);
			jsonResultObj.put("question", jsonResultArr);
			
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
	
	@Procedure(name = "horne.cdbg.sp.postProgramQuestions", mode = WRITE)
	public Stream<Output> postProgramQuestions(@Name("jsonRequest") String jsonRequest) throws Throwable
	{
		List<Output> stream = new ArrayList<Output>();
		
		JSONObject jsonResultObj = new JSONObject();
    	//JSONArray  jsonResultArr = new JSONArray();
    	
    	JSONObject jsonErrorObj = new JSONObject();
    	JSONArray  jsonErrorArr = new JSONArray();
 	
    	// Parse out the json input
    	JSONObject jsonInput = new JSONObject(jsonRequest);
    	
    	//String requestedBy = commonUtils.getJSONStringProperty(jsonInput, "requestedBy");
    	Long programId = commonUtils.getJSONLongProperty(jsonInput, "programId");
    	
    	Transaction tx = db.beginTx();
		
		try {
		
			// Create the program questionnaire
	    	String cypher = commonUtils.getCypherQuery(db, "POST_PROGRAM_QUESTIONNIONAIRE_QUERY");
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("programId", programId);
			db.execute(cypher, params);
	    	
	    	JSONArray questions = new JSONArray(jsonInput.get("question").toString());
	    	Iterator<Object> questionIter = questions.iterator();
	    	while(questionIter.hasNext()) {
	    		JSONObject question = new JSONObject(questionIter.next().toString());
	    		
	    		Long questionLibraryId = commonUtils.getJSONLongProperty(question, "questionLibraryId");
	    		Long sortId = commonUtils.getJSONLongProperty(question, "sortId");
	    		String questionText = commonUtils.getJSONStringProperty(question, "question");
	    		String phase = commonUtils.getJSONStringProperty(question, "phase");
	    		String category = commonUtils.getJSONStringProperty(question, "category");
	    		//String subCategory = commonUtils.getJSONStringProperty(question, "subCategory");
	    		String questionType = commonUtils.getJSONStringProperty(question, "questionType");
	    		String required = commonUtils.getJSONStringProperty(question, "required");
	    		//String active = commonUtils.getJSONStringProperty(question, "active");
	    		String appianMapping = commonUtils.getJSONStringProperty(question, "appianMapping");
	        	
	        	// Create the program question
	        	cypher = commonUtils.getCypherQuery(db, "POST_PROGRAM_QUESTION_QUERY");
	    		params = new HashMap<String, Object>();
	    		params.put("programId", programId);
	    		params.put("questionLibraryId", questionLibraryId);
	    		params.put("sortId", sortId);
	    		params.put("question", questionText);
	    		params.put("phase", phase);
	    		params.put("category", category);
	    		params.put("questionType", questionType);
	    		params.put("required", required);
	    		params.put("appianMapping", appianMapping);
	    		Result result = db.execute(cypher, params);
	    		if(!result.hasNext()) throw new Exception("There was an error creating the program question");
	    		Long questionId = commonUtils.getRowValueLong(result.next(), "questionId");
	    		
	    		if(question.has("options")) {
	    			
	    			JSONArray options = new JSONArray(question.get("options").toString());
	    			Iterator<Object> optionIter = options.iterator();
	    			while(optionIter.hasNext()) {
	    				JSONObject option = new JSONObject(optionIter.next().toString());
	    				
	    				Long questionOptionLibraryId = commonUtils.getJSONLongProperty(option, "questionOptionLibraryId");
	                    Long optionSortId = commonUtils.getJSONLongProperty(option, "sortId");
	                    String value = commonUtils.getJSONStringProperty(option, "value");
	                	
	                	// Create the program question option
	                	cypher = commonUtils.getCypherQuery(db, "POST_PROGRAM_QUESTION_OPTION_QUERY");
	            		params = new HashMap<String, Object>();
	            		params.put("programId", programId);
	            		params.put("questionLibraryId", questionLibraryId);
	            		params.put("questionOptionLibraryId", questionOptionLibraryId);
	            		params.put("questionId", questionId);
	            		params.put("sortId", optionSortId);
	            		params.put("value", value);
	            		db.execute(cypher, params);
	                }
	    			
	    		}
	    	}
	    	
			jsonResultObj.put("success", "true");
			
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
