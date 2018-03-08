package com.graphaware.horne.cdbg.sp.api;

import com.graphaware.api.json.JsonNode;
import com.graphaware.api.json.LongIdJsonNode;
import com.graphaware.common.log.LoggerFactory;

import com.graphaware.horne.cdbg.sp.StoredProcedures.Output;
import com.graphaware.horne.cdbg.sp.logic.StoredProceduresLogic;
import com.graphaware.horne.cdbg.sp.logic.CypherQueries;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;


@Controller
@RequestMapping("/horne")
public class StoredProceduresApi {

    //private static final Log LOG = LoggerFactory.getLogger(StoredProceduresApi.class);

    //@Context
    //public GraphDatabaseService db;
	
	@Context
	public Log log;
    
    private final GraphDatabaseService database;
    //private final TimedEventsBusinessLogic timedEventsLogic;

    @Autowired
    public StoredProceduresApi(
    		GraphDatabaseService database
    		) {
        this.database = database;
    }

    // http://localhost:7474/graphaware/horne/validateaddress?address1=28612 Blue Holly Lnnnn&postalcode=77494&city=katy&state=tx
    // http://{ServerNeo4j}/graphaware/horne/validateaddress?address1={address1}&postalcode={postalcode}&city={city}&state={tx}
    @RequestMapping(value = "/validateaddress", method = RequestMethod.GET)
    @ResponseBody
    public String validateAddress(
            @RequestParam(required = true) String address1,
            @RequestParam(required = false) String address2,
            @RequestParam(required = true) String postalcode,
            @RequestParam(required = true) String city,
            @RequestParam(required = true) String state
            ) {
    	
    	String result = "test";
    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.validateAddress(address1, city, state, postalcode);
   
    	return result;
    	
    }

    // http://localhost:7474/graphaware/horne/dob?address1=28612 Blue Holly Lnnnn&postalcode=77494&city=katy&state=tx
    @RequestMapping(value = "/dob", method = RequestMethod.GET)
    @ResponseBody
    public String dOB(
            @RequestParam(required = true) String address1,
            @RequestParam(required = false) String address2,
            @RequestParam(required = true) String postalcode,
            @RequestParam(required = true) String city,
            @RequestParam(required = true) String state
            ) {
    	
    	String result = "Failed";
    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.dOB(address1, city, state, postalcode);
   
    	return result;
    	
    }
    
    // http://localhost:7474/graphaware/horne/getauditfieldchanges?requestedby={username}&casetextid={CaseID}&field={field}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    // http://localhost:7474/graphaware/horne/getauditfieldchanges?requestedby={username}&casetextid=TX-FL16-01368&field={field}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    // http://ec2-54-245-131-128.us-west-2.compute.amazonaws.com:7474/graphaware/horne/getauditfieldchanges?requestedby={username}&casetextid=TX-FL16-01368&field={field}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
/*    @RequestMapping(value = "/getauditfieldchanges", method = RequestMethod.GET)
    @ResponseBody
    public String getAuditFieldChanges(
    		@RequestParam(required = true) String requestedby,
            @RequestParam(required = true) String casetextid,
            @RequestParam(required = true) String field,
            @RequestParam(required = true) String datefrom,
            @RequestParam(required = true) String dateto
            ) throws ParseException {
    	
    	String result = "Failed";
    	SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.getAuditFieldChanges(requestedby, casetextid, field, df.parse(datefrom), df.parse(dateto));
   
    	return result;
    	
    }  
    */
    // http://localhost:7474/graphaware/horne/getauditevents?requestedby={username}&casetextid={CaseID}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    // http://{Neo4jServer}:7474/graphaware/horne/getauditevents?requestedby={username}&casetextid={CaseID}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    @RequestMapping(value = "/getauditevents", method = RequestMethod.GET)
    @ResponseBody
    public String getAuditEvents(
    		@RequestParam(required = true) String requestedby,
            @RequestParam(required = true) String casetextid,
            @RequestParam(required = true) String datefrom,
            @RequestParam(required = true) String dateto
            ) throws ParseException {
    	
    	String result = "Failed";
    	SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.getAuditEvents(requestedby, casetextid, df.parse(datefrom), df.parse(dateto));
   
    	return result;
    	
    }      
    
    // http://localhost:7474/graphaware/horne/getaudittasks?requestedby={username}&casetextid={CaseID}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    // http://localhost:7474/graphaware/horne/getaudittasks?requestedby={username}&casetextid=TX-FL16-01368&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    // http://{Neo4jServer}:7474/graphaware/horne/getaudittasks?requestedby={username}&casetextid={CaseID}&datefrom=01/01/2018 22:59:59&dateto=01/01/2018 22:59:59
    @RequestMapping(value = "/getaudittasks", method = RequestMethod.GET)
    @ResponseBody
    public String getAuditTasks(
    		@RequestParam(required = true) String requestedby,
            @RequestParam(required = true) String casetextid,
            @RequestParam(required = true) String datefrom,
            @RequestParam(required = true) String dateto
            ) throws ParseException {
    	
    	String result = "Failed";
    	SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.getAuditTasks(requestedby, casetextid, df.parse(datefrom), df.parse(dateto));
   
    	return result;
    	
    }      
    
    
    // http://localhost:7474/graphaware/horne/reasigncasemanager?caseid=TX-FL16-01368&newcasemanagerfirstname=John&newcasemanagerlastname=Doe&newcaseManagerUsername=username&submittedby=submitter
    // http://{Neo4jServer}:7474/graphaware/horne/reasigncasemanager?caseid={caseId}&newcasemanagerfirstname={firstName}&newcasemanagerlastname={lastName}&newcaseManagerUsername={username}&submittedby={submitter}
    @RequestMapping(value = "/reasigncasemanager", method = RequestMethod.GET)
    @ResponseBody
    public String reasignCaseManager(
    		@RequestParam(required = true) String caseid,
    		@RequestParam(required = true) String newcasemanagerfirstname,
    		@RequestParam(required = true) String newcasemanagerlastname,
    		@RequestParam(required = true) String newcaseManagerUsername,
    		@RequestParam(required = true) String submittedby
            ) {
    	
    	String result = "test";
    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.reasignCaseManager(caseid, newcasemanagerfirstname, newcasemanagerlastname, newcaseManagerUsername, submittedby);
   
    	return result;
 
    }
    
    
    
    // http://localhost:7474/graphaware/horne/DetermineDOBForCase?caseid=CASEID&submittedby=submitter
    @RequestMapping(value = "/DetermineDOBForCase", method = RequestMethod.GET)
    @ResponseBody
    public String determineDOBForCase(
    		@RequestParam(required = true) String caseid,
    		@RequestParam(required = true) String submittedby
            ) {
    	
    	String result = "test";
    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.determineDOBForCase(caseid, submittedby);
   
    	return result;
 
    }
    
 // http://localhost:7474/graphaware/horne/PostCaseDocument?caseid=California-Andrew-5541&documenturl='http://dataquadrant.com?aaa=bb&vv=hhhh'&documenttype=ggggggg&submittedby=submitter
    @RequestMapping(value = "/PostCaseDocument", method = RequestMethod.GET)
    @ResponseBody
    public String postCaseDocument(
    		@RequestParam(required = true) String caseid,
    		@RequestParam(required = true) String documenturl,
    		@RequestParam(required = true) String documenttype,
    		@RequestParam(required = true) String submittedby
            ) {
    	
    	String result = "test";
    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	result = logic.postCaseDocument(caseid, documenturl, documenttype, submittedby);
   
    	return result;
 
    }
    
    // http://localhost:7474/graphaware/horne/getstatelist
    // http://ec2-54-244-162-242.us-west-2.compute.amazonaws.com:7474/graphaware/horne/getstateagencylist
    @RequestMapping(value = "/getstatelist", method = RequestMethod.GET)
    @ResponseBody
    public String getStateList() 
    {
    	
    	JSONArray  jarr = new JSONArray();
    	
    	Transaction tx = database.beginTx();
		
		try {
			/*
    		Programs programs = new Programs();
    		programs.setDB(database);
    		Stream<GetCityByProgram> result = programs.getCityByProgram(programid);
			 */
			
    		//Iterator<GetCityByProgram> iterator = result.iterator();
			
			Result result = database.execute(CypherQueries.GET_STATE_LIST);
			
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				JSONObject json = new JSONObject();
    			json.put("StateName", row.get("StateName"));
    			json.put("Abbreviation", row.get("Abbreviation"));
 			
    			jarr.put(json);
	
			}
  		
    		
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		tx.terminate();
    		log.error(ex.getMessage());
    		return ex.getMessage();
    	} finally {
    		tx.close();
    	}
		
		return jarr.toString();
    	
    }    
    
    
 // http://localhost:7474/graphaware/horne/getstateagencylist
    @RequestMapping(value = "/getstateagencylist", method = RequestMethod.GET)
    @ResponseBody
    public String getStateAgencyList(
    		@RequestParam(required = false) String state) 
    {
    	
    	JSONArray  jarr = new JSONArray();
    	
    	Transaction tx = database.beginTx();
		
		try {
			
			Result result;
			if (state != null)
			{
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("state1", state);
				params.put("state2", state);
				result = database.execute(CypherQueries.GET_STATE_AGENCY_PARAM_LIST, params);
			}
			else
			{
				result = database.execute(CypherQueries.GET_STATE_AGENCY_LIST);
			}
			
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				JSONObject json = new JSONObject();
				json.put("AgencyName", row.get("AgencyName"));
    			json.put("StateName", row.get("StateName"));
    			json.put("StateAbbreviation", row.get("StateAbbreviation"));
 			
    			jarr.put(json);
	
			}
  		
    		
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		tx.terminate();
    		log.error(ex.getMessage());
    		return ex.getMessage();
    	} finally {
    		tx.close();
    	}
		
		return jarr.toString();
    	
    }    
    
    // http://localhost:7474/graphaware/horne/getfederalagencylist
    // http://ec2-54-244-162-242.us-west-2.compute.amazonaws.com:7474/graphaware/horne/getfederalagencylist
    @RequestMapping(value = "/getfederalagencylist", method = RequestMethod.GET)
    @ResponseBody
    public String getFederalAgencyList() 
    {
    	
    	JSONArray  jarr = new JSONArray();
    	
    	Transaction tx = database.beginTx();
		
		try {

			Result result = database.execute(CypherQueries.GET_FEDERAL_AGENCY_LIST);
		
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				JSONObject json = new JSONObject();
    			json.put("AgencyName", row.get("AgencyName"));
 			
    			jarr.put(json);
	
			}
  		
    		
    		
    		tx.success();
    			
    	} catch (Throwable ex) {
    		tx.terminate();
    		log.error(ex.getMessage());
    		return ex.getMessage();
    	} finally {
    		tx.close();
    	}
		
		return jarr.toString();
    	
    }    
    
    // http://localhost:7474/graphaware/horne/getprogramtypelist
    // http://ec2-54-244-162-242.us-west-2.compute.amazonaws.com:7474/graphaware/horne/getprogramtypelist
    @RequestMapping(value = "/getprogramtypelist", method = RequestMethod.GET)
    @ResponseBody
    public String getProgramTypeList() 
    {
    	StoredProceduresLogic logic = new StoredProceduresLogic(database);
    	return logic.getProgramTypeList();
    }
    
    
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Map<String, String> handleIllegalArgument(IllegalArgumentException e) {
        // LOG.warn("Bad Request: " + e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Map<String, String> handleIllegalState(IllegalStateException e) {
        // LOG.warn("Bad Request: " + e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ResponseBody
    public Map<String, String> handleNotFound(NotFoundException e) {
        // LOG.warn("Not Found: " + e.getMessage(), e);
        return Collections.singletonMap("message", e.getMessage());
    }
   


}
