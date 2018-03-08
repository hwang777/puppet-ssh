package com.graphaware.horne.cdbg.sp.logic;

public class CypherQueries {

	public static String POST_CASE_DOCUMENT= "CREATE (a:Document{CaseID:{caseid}, DocumentUrl:{documenturl}, "
			+ "DocumentType:{documenttype}, SubmittedBy:{submittedby}, ToBeDeleted:1 }) "
			 + "WITH a MATCH (b:Case) WHERE b.CaseID = a.CaseID MERGE (b)-[:HAS_DOCUMENT]->(a)";
	
	public static String REMOVE_CURENT_MANAGER = "MATCH (a:Case)-[rr:IS_REVIEWED_BY]->(:Person)  "
			+ "WHERE a.CaseID = {caseId} DELETE rr";
	
	public static String REASIGN_CASE_MANAGER = "MATCH (a:Case)--(b:CasePhase)--(c:Task) "+ 
			"WHERE a.CaseID = {caseId} AND c.AppianTaskStatus <> 'Completed' "+ 
			"SET b.AppianTaskAssignee = {newCaseManagerUsername}  "+ 
			"SET c.AppianTaskAssignee =b.AppianTaskAssignee "+ 
			"SET c.SubmittedBy={submittedby}  "+ 
			"SET a.SubmittedBy=b.AppianTaskAssignee "+ 
			"WITH a,b,c MERGE (a)-[:IS_REVIEWED_BY]->(:Person { "+ 
			"FirstName:{newCaseManagerFirstName},  "+ 
			"LastName: {newCaseManagerLastName},  "+ 
			"Username: b.AppianTaskAssignee,  "+ 
			"IsCaseManager:1, "+ 
			"SubmittedBy:c.SubmittedBy })";
	
	public static String GET_STATE_LIST = "MATCH (a:State) "
			+ "RETURN a.name as StateName, a.Abbreviation as Abbreviation order by a.name";
	
	public static String GET_STATE_AGENCY_PARAM_LIST = "OPTIONAL MATCH (a:Agency {AgencyType:'StateAgency'}), (b:State) " + 
			"WHERE a.StateAbbreviation = b.Abbreviation " + 
			"AND (b.Abbreviation = {state1} or b.name = {state2}) " + 
			"RETURN a.name as AgencyName, b.name as StateName, b.Abbreviation as StateAbbreviation order by a.name";
	
	public static String GET_STATE_AGENCY_LIST = "OPTIONAL MATCH (a:Agency {AgencyType:'StateAgency'}), (b:State) WHERE a.StateAbbreviation = b.Abbreviation RETURN a.name as AgencyName, b.name as StateName, b.Abbreviation as StateAbbreviation ORDER by a.name" + 
			"";
	public static String GET_FEDERAL_AGENCY_LIST =  "MATCH (a:Agency {AgencyType:'FederalAgency'}) RETURN a.name as AgencyName ORDER BY a.name " + 
			"";
	
	public static String GET_PROGRAM_TYPE_LIST = "MATCH (a:ProgramType) RETURN a.name as ProgramTypeName ORDER BY a.name";
	
	public static String GET_AUDIT_TASKS ="match (c:Case)--(p:CasePhase)--(t:Task) where c.CaseID= {caseid} " +
			"match (x:_GA_Audit_NodeUpdated)-->(tx:_GA_Audit_Tx) where x.uuid = t.uuid  " +
			"return  " +
			"x.Name as taskName, " +
			"x.AppianTaskAssignee as entryUserId, " +
			"x.AppianTaskId as taskId, " +
			"x.AppianTaskStatus as taskStatus,  " +
			"apoc.date.format(tx.timestamp, 'ms', 'yyyy/MM/dd HH:mm:ss') as entryDate " +
			"order by tx.timestamp desc limit 20";
	
	public static String GET_AUDIT_FILDCHANGES = "match (c:Case) "
			+ "where "
			+ "{caseid} = '' or c.CaseID = {caseid} " + 
			"match (x:_GA_Audit_NodeUpdated)-->(tx:_GA_Audit_Tx) where x.uuid = c.uuid " + 
			"return " + 
			"c.CaseID as caseTextId, " + 
			"id(c) as caseId, " + 
			"'Status' as field, " + 
			"c.Status as startvalue, " + 
			"c.Status as endValue, " + 
			"'username' as changedBy, " +
			"apoc.date.format(tx.timestamp, 'ms', 'yyyy/MM/dd HH:mm:ss') as transactionDate "
			+ "order by tx.timestamp desc";
	
	public static String GET_AUDIT_FIELDCHANGES2 = "match (c:Case) \r\n" + 
			"where \r\n" + 
			"{caseid1} = '' or c.CaseID = {caseid2} \r\n" + 
			"match (x:_GA_Audit_NodeUpdated)-->(tx:_GA_Audit_Tx) where x.uuid = c.uuid\r\n" + 
			"with c,x,tx\r\n" + 
			"optional match (x:_GA_Audit_NodeUpdated)-->(xx:_GA_Audit_NodeUpdated)\r\n" + 
			"return\r\n" + 
			"\r\n" + 
			"c.CaseID as caseTextId,\r\n" + 
			"coalesce(x.SubmittedBy,'unknown') as SubmittedBy,"
			+ "id(c) as caseId,\r\n";
	
	
	
	public static String GET_AUDIT_FIELDCHANGES3 = ""
			+ " apoc.date.format(tx.timestamp, 'ms', 'yyyy/MM/dd HH:mm:ss') as transactionDate\r\n" + 
			"order by tx.timestamp desc";
	
	public static String GET_CASE_FIELDS = "MATCH (p:Case) WITH DISTINCT keys(p) AS keys\r\n" + 
			"UNWIND keys AS keyslisting WITH DISTINCT keyslisting AS allfields\r\n" + 
			"RETURN allfields;\r\n" + 
			"";
	
	
}
