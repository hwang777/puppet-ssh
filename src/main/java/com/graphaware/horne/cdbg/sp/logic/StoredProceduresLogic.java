package com.graphaware.horne.cdbg.sp.logic;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import com.graphaware.horne.cdbg.sp.logic.CypherQueries;
import com.graphaware.horne.cdbg.sp.Agencies;
import com.graphaware.horne.cdbg.sp.ChangeLogger;
import com.graphaware.horne.cdbg.sp.StoredProcedures.Output;
import com.graphaware.common.log.LoggerFactory;

public class StoredProceduresLogic {
	
	private final GraphDatabaseService database;
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	
	
	public StoredProceduresLogic(GraphDatabaseService database) {
        this.database = database;
    }
	
	
	public List<Node> getAddresses(String someParameter) {
        List<Node> nodes;
        try (Transaction tx = database.beginTx()) {
            //nodes = timeTree.getInstants(startTimeInstant, endTimeInstant);
            tx.success();
        }
        return null;
    }
	
	public String validateAddress(String addressLine1,
    		String city,
    		String state,
    		String postalCode)
	{
		RestTemplate restT = new RestTemplate();
    	Map<String, String> vars = new HashMap<String, String>();
    	//vars.put("url", "http://dev.virtualearth.net/REST/v1/Locations?country");
    	vars.put("addr", addressLine1);
    	vars.put("city", city);
    	//vars.put("state", state);
    	vars.put("postalCode", postalCode);
    	vars.put("key", "AlttSmhXIZgMUb_rxVvDQRLpo_JJVSWATUuMcS_ppY15PO5jxPVgiIqTvEcHot2v");
    	
    	String result = "Failed";
    	result = restT.getForObject("http://dev.virtualearth.net/REST/v1/Locations?countryRegion=US&locality={city}&postalCode={postalCode}&addressLine={addr}&key={key}", 
		String.class, vars);  
    	
    	JSONObject jsonObject = new JSONObject(result);
        JSONArray resourceSets = jsonObject.optJSONArray("resourceSets");
        JSONObject resource =  resourceSets.optJSONObject(0).optJSONArray("resources").getJSONObject(0);
        String name = resource.getString("name");
    	JSONObject address = resource.optJSONObject("address");
    	
    	JSONObject retObj = new JSONObject();
    	JSONObject retAddr = new JSONObject();
    	retAddr = retAddr.put("address", resource.optJSONObject("address").getString("addressLine"));
    	retAddr = retAddr.put("address1", "");
    	retAddr = retAddr.put("city", resource.optJSONObject("address").getString("locality"));
    	retAddr = retAddr.put("state", resource.optJSONObject("address").getString("adminDistrict"));
    	retAddr = retAddr.put("county", resource.optJSONObject("address").getString("adminDistrict2"));
    	retAddr = retAddr.put("country", resource.optJSONObject("address").getString("countryRegion"));
    	retAddr = retAddr.put("zip", resource.optJSONObject("address").getString("postalCode"));
    	retAddr = retAddr.put("formattedAddress", resource.optJSONObject("address").getString("formattedAddress"));
    	retAddr = retAddr.put("geocodePoints", resource.optJSONArray("geocodePoints").optJSONObject(0).optJSONArray("coordinates"));
    	//formattedAddress
    	retObj = retObj.put("success", true);
    	retObj = retObj.put("matchedAddress", retAddr);
    	// result = address.toString();
    	result = retObj.toString();	
		return result;
	}

	public String dOB(String addressLine1,
    		String city,
    		String state,
    		String postalCode)
	{
		RestTemplate restT = new RestTemplate();
    	Map<String, String> vars = new HashMap<String, String>();
    	//vars.put("url", "http://dev.virtualearth.net/REST/v1/Locations?country");
    	vars.put("addr", addressLine1);
    	vars.put("city", city);
    	//vars.put("state", state);
    	vars.put("postalCode", postalCode);
    	
    	String result = "Failed";
    	//result = restT.getForObject("http://dev.virtualearth.net/REST/v1/Locations?countryRegion=US&locality={city}&postalCode={postalCode}&addressLine={addr}&key={key}", 
		//String.class, vars);  
    	
//   	JSONObject jsonObject = new JSONObject();
//        JSONArray resourceSets = jsonObject.optJSONArray("resourceSets");
//        JSONObject resource =  resourceSets.optJSONObject(0).optJSONArray("resources").getJSONObject(0);
//        String name = resource.getString("name");
//    	JSONObject address = resource.optJSONObject("address");
    	
    	JSONObject retObj = new JSONObject();
    	//JSONObject = new JSONObject();
    	JSONArray aBenefits = new JSONArray();
    	JSONObject oBenefit;
    	
    	oBenefit = new JSONObject();
    	oBenefit.put("provider", "FEMA");
    	oBenefit.put("providerContact", "");
    	oBenefit.put("type", "");
    	oBenefit.put("amount", "");
    	oBenefit.put("referenceId", "");
    	
    	aBenefits.put(oBenefit);
    	
    	oBenefit = new JSONObject();
    	oBenefit.put("provider", "NFID");
    	oBenefit.put("providerContact", "");
    	oBenefit.put("type", "");
    	oBenefit.put("amount", "");
    	oBenefit.put("referenceId", "");
    	
    	aBenefits.put(oBenefit);
    	
    	
    	/*
    	retAddr = retAddr.put("address", resource.optJSONObject("address").getString("addressLine"));
    	retAddr = retAddr.put("address1", "");
    	retAddr = retAddr.put("city", resource.optJSONObject("address").getString("locality"));
    	retAddr = retAddr.put("state", resource.optJSONObject("address").getString("adminDistrict"));
    	retAddr = retAddr.put("county", resource.optJSONObject("address").getString("adminDistrict2"));
    	retAddr = retAddr.put("country", resource.optJSONObject("address").getString("countryRegion"));
    	retAddr = retAddr.put("zip", resource.optJSONObject("address").getString("postalCode"));
    	retAddr = retAddr.put("formattedAddress", resource.optJSONObject("address").getString("formattedAddress"));
    	retAddr = retAddr.put("geocodePoints", resource.optJSONArray("geocodePoints").optJSONObject(0).optJSONArray("coordinates"));
    	*/
    	
    	retObj = retObj.put("success", "true");
    	retObj = retObj.put("benefits", aBenefits);
    	// result = address.toString();
    	result = retObj.toString();	
		return result;
	}

	public List<String> getCaseFields(Transaction tx)
	{
		List<String> fields = new ArrayList<String>();
		Result resultCypher;
		resultCypher = database.execute(CypherQueries.GET_CASE_FIELDS);
		while(resultCypher.hasNext())
		{
			Map<String, Object> row = resultCypher.next();
			fields.add(row.get("allfields").toString());
		}
        
		return fields;
	}
	
	
	
	public String getAuditFieldChanges(
			String requestedby,
			String caseTextId,
			String field,
			String datefrom, String dateto
			)
	{
    	Map<String, Object> vars = new HashMap<String, Object>();
    	//if (!caseTextId.isEmpty())
    	vars.put("caseid1", caseTextId);
    	vars.put("caseid2", caseTextId);
    	
    	vars.put("field", field);
    	vars.put("datefrom", datefrom);
    	if (dateto != null)
    		vars.put("dateto", dateto);

    	Transaction tx = database.beginTx();
    	String returnResult = "Failed";
    	JSONObject retObj = new JSONObject();
    	JSONArray arObj = new JSONArray();
    	
    	List<String> fields = getCaseFields(tx);
    	
    	try
    	{
			Result resultCypher;
			String query = CypherQueries.GET_AUDIT_FIELDCHANGES2;
			for (String f: fields)
	    	{
				if (f.toLowerCase().equals("uuid") || f.toLowerCase().equals("submittedby"))
					continue;				
				query += "x." + f + " as " + f + ", ";
				query += "xx." + f + " as prev_" + f + ", ";
	    	}
			query += CypherQueries.GET_AUDIT_FIELDCHANGES3;
			
			resultCypher = database.execute(query, vars);
			
			while(resultCypher.hasNext())
			{
				String changeDescription = "";
				Map<String, Object> row = resultCypher.next();
				
				boolean isLastRow = true;
				for (String f: fields)
				{
					if (row.get("prev_"+f) != null)
					{
						isLastRow = false;
						break;
					}
				}
				
				for (String f: fields)
				{
					
					if (row.get(f)== null && row.get("prev_"+f) == null)
						continue;
					
					if (f.toLowerCase().equals("uuid") || f.toLowerCase().equals("submittedby"))
						continue;
		
					if (!isLastRow && row.get(f)!= null && row.get("prev_"+f) == null)
					{
						changeDescription += "New proprety added: " + f + " Value: '" + row.get(f) + "'. ";;
					}
					
					if (row.get(f)== null && row.get("prev_"+f) != null)
					{
						changeDescription += "Property daleted: " + f + ". ";
					}
					
					if (row.get(f) != null && row.get("prev_"+f) != null && 
							!row.get(f).toString().equals(row.get("prev_"+f).toString()))
					{
						changeDescription += "Property: " + f + " changed value From: '" + row.get("prev_"+f).toString() + "' To: '" + row.get(f).toString() + "'. ";
					}
				} // for
				
				
		  		JSONObject jsonObj;
		    	jsonObj = new JSONObject();
		    	jsonObj.put("changeDate", checkForNull(row.get("transactionDate")));
		    	jsonObj.put("changedBy", checkForNull(row.get("SubmittedBy")));
		    	jsonObj.put("caseId", checkForNull(row.get("caseId")));
		    	jsonObj.put("caseTextId", checkForNull(row.get("caseTextId")));
		    	jsonObj.put("field", "Status");
		    	jsonObj.put("startvalue", checkForNull(row.get("prev_Status")));
		    	jsonObj.put("endValue", checkForNull(row.get("Status")));
		    	jsonObj.put("changeDescription", changeDescription);
		    	arObj.put(jsonObj);
		    	
		    	//System.out.println(changeDescription);
			}
	        
	        tx.success();	        
	    }
		catch (Exception x)
		{
			tx.failure();
			log.error(x.getMessage(), x.toString());
			return x.getMessage() ;
		}
    	finally
    	{
    		tx.close();
    	}
    	
    	retObj = retObj.put("success", "true");
    	retObj = retObj.put("changeHistory", arObj);
    	
    	returnResult = retObj.toString();
    	return returnResult;
	}
	
	public String getAuditEvents(
			String requestedBy,
			String caseTextId,
			Date dateFrom,
			Date dateTo
			)
	{
		RestTemplate restT = new RestTemplate();
    	Map<String, String> vars = new HashMap<String, String>();

    	vars.put("caseid", caseTextId);
    	vars.put("datefrom", String.valueOf(dateFrom.getTime()));
    	vars.put("dateto", String.valueOf(dateTo.getTime()));
    	
    	String result = "Failed";

    	JSONObject retObj = new JSONObject();
    	
    	JSONArray aChanges = new JSONArray();
    	JSONObject jsonObj;
  	
    	jsonObj = new JSONObject();
    	jsonObj.put("eventDate", "02/08/2018 22:10:55");
    	jsonObj.put("changedBy", "username");
    	jsonObj.put("caseId", "12345");
    	jsonObj.put("caseTextId", caseTextId);
    	jsonObj.put("eventType", "Event Type");
    	jsonObj.put("eventDescription", "This will be longer than 255 characters");   	
    	aChanges.put(jsonObj);
    
    	retObj = retObj.put("success", "true");
    	retObj = retObj.put("changeHistory", aChanges);
    	
    	result = retObj.toString();	
		return result;
	}
	
	public String getAuditTasks(
			String requestedby,
			String caseTextId,
			Date dateFrom,
			Date dateTo
			)
	{
    	Map<String, Object> vars = new HashMap<String, Object>();

    	vars.put("caseid", caseTextId);
    	//vars.put("datefrom", String.valueOf(dateFrom.getTime()));
    	//vars.put("dateto", String.valueOf(dateTo.getTime()));
 
    	Transaction tx = database.beginTx();
    	String returnResult = "Failed";
    	JSONObject retObj = new JSONObject();
    	JSONArray arObj = new JSONArray();
    	try
    	{
			Result resultCypher;
			resultCypher = database.execute(CypherQueries.GET_AUDIT_TASKS, vars);
			
			while(resultCypher.hasNext())
			{
				Map<String, Object> row = resultCypher.next();
		  		JSONObject jsonObj;
		    	jsonObj = new JSONObject();
		    	jsonObj.put("changedBy", "username");
		    	jsonObj.put("caseTextId", caseTextId);
		    	jsonObj.put("taskId", row.get("taskId"));
		    	jsonObj.put("taskName", row.get("taskName"));
		    	
		    	String entryUserId = checkForNull(row.get("entryUserId"));
		    	String entryDate = checkForNull(row.get("entryDate"));
		    	jsonObj.put("entryUserId", entryUserId);
		    	jsonObj.put("entryUserName", entryUserId);
		    	jsonObj.put("entryDate", entryDate);
		    	jsonObj.put("exitUserId", entryUserId);
		    	jsonObj.put("exitUserName", entryUserId);
		    	jsonObj.put("exitDate", entryDate);
		    	arObj.put(jsonObj);
			}
	        
	        tx.success();	        
	    }
		catch (Exception x)
		{
			tx.failure();
			log.error(x.getMessage(), x.toString());
			return x.getMessage() ;
		}
    	finally
    	{
    		tx.close();
    	}
    	
    	retObj = retObj.put("success", "true");
    	retObj = retObj.put("changeHistory", arObj);
    	
    	returnResult = retObj.toString();
    	return returnResult;
 
	}
	
	public String getProgramTypeList()
	{

       	Transaction tx = database.beginTx();
    	String returnResult = "Failed";
    	JSONArray arObj = new JSONArray();
    	try
    	{
			Result resultCypher;
			resultCypher = database.execute(CypherQueries.GET_PROGRAM_TYPE_LIST);
			while(resultCypher.hasNext())
			{
				Map<String, Object> row = resultCypher.next();
		  		JSONObject jsonObj;
		    	jsonObj = new JSONObject();
		    	jsonObj.put("ProgramTypeName", row.get("ProgramTypeName"));
		    	arObj.put(jsonObj);
			}
	        
	        tx.success();	        
	    }
		catch (Exception x)
		{
			tx.failure();
			log.error(x.getMessage(), x.toString());
			return x.getMessage() ;
		}
    	finally
    	{
    		tx.close();
    	}
    	
    	returnResult = arObj.toString();
    	return returnResult;
	}
	
	
	
	public String reasignCaseManager(
    		String caseId,
    		String newCaseManagerFirstName,
    		String newCaseManagerLastName,
    		String newCaseManagerUsername,
    		String submittedBy
    		)
    {
		String response = "Success";
		Map<String, Object> params2 = new HashMap<String, Object>();
		params2.put("caseId", caseId);
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("caseId", caseId);
		params.put("newCaseManagerFirstName", newCaseManagerFirstName);
		params.put("newCaseManagerLastName", newCaseManagerLastName);
		params.put("newCaseManagerUsername", newCaseManagerUsername);
		params.put("submittedby", submittedBy);

		try (Transaction tx = database.beginTx())
		{
			Result result2 = database.execute(CypherQueries.REMOVE_CURENT_MANAGER, params2);
			Result result = database.execute(CypherQueries.REASIGN_CASE_MANAGER, params);
			//LOG.info(result);
	        tx.success();
	        
	        return response;
	    }
		catch (Exception x)
		{
			//tx.terminate();
			log.error(x.getMessage(), x.toString());
			return x.getMessage() ;
		}
    }
    
	
	public String determineDOBForCase(
    		String caseId,
    		String submittedBy
    		)
    {
    	String result = "Success";
    	return result;
    }
	
	public String postCaseDocument(
    		String caseid,
    		String documenturl,
    		String documenttype,
    		String submittedby
            ) 
	{
		
		String response = "Success";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("caseid", caseid);
		params.put("documenturl", documenturl);
		params.put("documenttype", documenttype);
		params.put("submittedby", submittedby);
		//Transaction tx = database.beginTx();
		try (Transaction tx = database.beginTx())
		{
	        //nodes = timeTree.getInstants(startTimeInstant, endTimeInstant);
			Result result = database.execute(CypherQueries.POST_CASE_DOCUMENT, params);
			//LOG.info(result);
	        tx.success();
	        
	        return response;
	    }
		catch (Exception x)
		{
			//tx.terminate();
			log.error(x.getMessage(), x.toString());
			return x.getMessage() ;
		}
	}	
		
	// This checks if the node id is null and returns a null node
		private String checkForNull (Object value) 
		{
			if(value == null)
				return "";
			else
				return value.toString();
		}
	    
		
} // class
