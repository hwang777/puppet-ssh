package com.graphaware.horne.cdbg.sp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;

import com.graphaware.horne.cdbg.sp.logic.MelissaData;
import com.graphaware.horne.cdbg.sp.logic.StoredProceduresLogic;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.procedure.Mode.SCHEMA;

/**
 
 */
public class StoredProcedures
{
    // Only static fields and @Context-annotated fields are allowed in
    // Procedure classes. This static field is the configuration we use
    // to create full-text indexes.
    private static final Map<String,String> FULL_TEXT =
            stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" );

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;
    
    public static class Output {
    	public String result;
    }

    // call horne.cdbg.sp.ValidateAddress('28623 Blue Holly Ln', 'Katy', 'TX','77494')
    @Procedure(value = "horne.cdbg.sp.ValidateAddress", mode = Mode.WRITE)
    @Description("Validate a given address.")
    public Stream<Output> validateAddress(
    		@Name("RequestedBy")  String requestedBy,
    		@Name("AddressLine1") String addressLine1,
    		@Name("AddressLine2") String addressLine2,
    		@Name("City") String city,
    		@Name("State") String state,
    		@Name("PostalCode") String postalCode
    		)
    {
    	String result = "";
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.validateAddress(addressLine1, city, state, postalCode);
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    // call horne.cdbg.sp.ValidateAddressMelissa('username','28623 Blue Holly Ln','', 'Katy', 'TX','77494')
    @Procedure(value = "horne.cdbg.sp.ValidateAddressMelissa", mode = Mode.WRITE)
    @Description("Validate a given address using a Melissa Data API call.")
    public Stream<Output> validateAddressMelissa(
    		@Name("RequestedBy")  String requestedBy,
    		@Name("AddressLine1") String addressLine1,
    		@Name("AddressLine2") String addressLine2,
    		@Name("City") String city,
    		@Name("State") String state,
    		@Name("PostalCode") String postalCode
    		)
    {
    	String result = "";
    	MelissaData logic = new MelissaData(db);
    	result = logic.validateAddress(addressLine1, addressLine2, city, state, postalCode);
    			
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    @Procedure(value = "horne.cdbg.sp.DOB", mode = Mode.WRITE)
    @Description("Return Duplicate Of Benefits")
    public Stream<Output> dOB(
    		@Name("RequestedBy")  String requestedBy,
    		@Name("AddressLine1") String addressLine1,
    		@Name("AddressLine2") String addressLine2,
    		@Name("City") String city,
    		@Name("State") String state,
    		@Name("PostalCode") String postalCode
    		)
    {
    	String result = "";
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.dOB(addressLine1, city, state, postalCode);
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    // call horne.cdbg.sp.GetAuditFieldChanges('requestedby', 'TX-FL16-01368', 'Status', '01/01/2018 22:52:52', '01/02/2018 22:52:52')
    // call horne.cdbg.sp.GetAuditFieldChanges("DateFrom:'01/01/2018 22:52:52'", "DateTo:'01/01/2018 22:52:52'")
    @Procedure(value = "horne.cdbg.sp.GetAuditFieldChanges", mode = Mode.WRITE)
    @Description("Return audit track of chanhes for a case and field")
    public Stream<Output> getAuditFieldChanges(
    		@Name(value="RequestedBy", defaultValue="unknown") String requestedby,
    		@Name(value="CaseID", defaultValue="") String casetextid,
    		@Name(value="Field", defaultValue="") String field,
    		@Name(value="DateFrom", defaultValue="") String datefrom,
    		@Name(value="DateTo", defaultValue="") String dateto
    		) throws ParseException
    {
    	
    	
    	String result = "";
    	//SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    	
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	//result = logic.getAuditFieldChanges(requestedby, casetextid, field, df.parse(datefrom), df.parse(dateto));
    	result = logic.getAuditFieldChanges(requestedby, casetextid, field, datefrom, dateto);
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    // call horne.cdbg.sp.GetAuditEvents('username', 'TX-FL16-01368', '01/16/2017 22:55:55', '01/16/2017 23:55:55')
    @Procedure(value = "horne.cdbg.sp.GetAuditEvents", mode = Mode.WRITE)
    @Description("Return audit track of chanhes for CDBG Events")
    public Stream<Output> getAuditEvents(
    		@Name(value="RequestedBy", defaultValue="unknown") String requestedby,
    		@Name(value="CaseID", defaultValue="") String casetextid,
    		@Name(value="DateFrom", defaultValue="") String datefrom,
    		@Name(value="DateTo", defaultValue="") String dateto
    		) throws ParseException
    {
    	
    	// @Name(value = "properties", defaultValue = "[]")
    	String result = "";
    	SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	Date from = df.parse(datefrom);
    	Date to = null;
    	if (!dateto.isEmpty())
    		to = df.parse(dateto);
    	
    	result = logic.getAuditEvents(requestedby, casetextid, from, to);
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    // call horne.cdbg.sp.GetAuditTasks({username}, 'TX-FL16-01368', '01/01/2018 22:52:52', '01/02/2018 22:52:52')
    @Procedure(value = "horne.cdbg.sp.GetAuditTasks", mode = Mode.WRITE)
    @Description("Return audit track of chanhes for CDBG Tasks")
    public Stream<Output> getAuditTasks(
    		@Name("RequestedBy") String requestedby,
    		@Name("CaseID") String casetextid,
    		@Name("DateFrom") String datefrom,
    		@Name("DateTo") String dateto
    		) throws ParseException
    {
    	
    	
    	String result = "";
    	SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.getAuditTasks(requestedby, casetextid, df.parse(datefrom), df.parse(dateto));
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    @Procedure(value = "horne.cdbg.sp.GetProgramTypeList", mode = Mode.WRITE)
    @Description("Return CDBG Program Type list")
    public Stream<Output> getProgramTypeList() throws ParseException
    {
    	
    	
    	String result = "";
    	//SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.getProgramTypeList();
    	Output o = new Output();
    	o.result = result;
    	
    
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    // This is for https://horne-llp.atlassian.net/browse/CC-352
    // e-assign case managers to perform an Intake Review
    // call horne.cdbg.sp.ReasignCaseManager('TX-FL16-01368', 'firstname01', 'lastname01', 'username', 'submiter')
    @Procedure(value = "horne.cdbg.sp.ReasignCaseManager", mode = Mode.WRITE)
    @Description("Re-assign case managers to perform an Intake Review")
    public Stream<Output> reasignCaseManager(
    		@Name("CaseId") String caseId,
    		@Name("NewCaseManagerFirstName") String newCaseManagerFirstName,
    		@Name("NewCaseManagerLasttName") String newCaseManagerLastName,
    		@Name("NewCaseManagerUsername") String newCaseManagerUsername,
    		@Name("SubmittedBy") String submittedBy
    		)
    {
    	String result = "Success";
    	
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.reasignCaseManager(caseId, newCaseManagerFirstName, newCaseManagerLastName, newCaseManagerUsername, submittedBy);
    	Output o = new Output();
    	o.result = result;
    	
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    /*
    
    @Procedure(value = "horne.cdbg.sp.GetStateList", mode = Mode.READ)
    @Description("Retrive the list of States. Name,Abbreviation")
    public Stream<State> GetStateList()
    {
    	List<GetCityByProgram> stream = new ArrayList<GetCityByProgram>();
		
		
		Result result = db.execute(CypherQueries.GET_CITY_BY_PROGRAM_QUERY);
			
		while(result.hasNext())
		{
			Map<String, Object> row = result.next();
			Node programNode = checkForNull(row.get("programId"));
			Node cityNode = checkForNull(row.get("cityId"));

			GetCityByProgram output = new GetCityByProgram(programNode,
					cityNode);
			
			stream.add(output);
		}
	
		return stream.stream();
    }
    
*/
    
    
	
    
    // This is for https://horne-llp.atlassian.net/browse/CC-441
    // Query the external DBs and determine the DOB for a given Case   
    @Procedure(value = "horne.cdbg.sp.DetermineDOBForCase", mode = Mode.WRITE)
    @Description("Query the external DBs and determine the DOB for a given Case")
    public Stream<Output> determineDOBForCase(
    		@Name("CaseId") String caseId,
    		@Name("SubmittedBy") String submittedBy
    		)
    {
    	String result = "Success";
    	
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.determineDOBForCase(caseId, submittedBy);
    	Output o = new Output();
    	o.result = result;
    	
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
    
    
    // 
    // Post a document for a Case
    @Procedure(value = "horne.cdbg.sp.PostCaseDocument", mode = Mode.WRITE)
    @Description("Query the external DBs and determine the DOB for a given Case")
    public Stream<Output> postCaseDocument(
    		@Name("CaseId") String caseId,
    		@Name("DocumentUrl") String documentUrl,
    		@Name("DocumentType") String documentType,
    		@Name("SubmittedBy") String submittedBy
    		)
    {
    	String result = "Success";
    	
    	StoredProceduresLogic logic = new StoredProceduresLogic(db);
    	result = logic.postCaseDocument(caseId, documentUrl, documentType, submittedBy);
    	Output o = new Output();
    	o.result = result;
    	
    	List<Output> list = new ArrayList<Output>();
    	list.add(o);
    	return list.stream();
    }
}
