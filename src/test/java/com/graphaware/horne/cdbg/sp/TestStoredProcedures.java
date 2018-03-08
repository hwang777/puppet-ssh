package com.graphaware.horne.cdbg.sp;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.Result;

import com.graphaware.horne.cdbg.sp.StoredProcedures;
import com.graphaware.horne.cdbg.sp.logic.MelissaData;
import com.graphaware.horne.cdbg.sp.logic.StoredProceduresLogic;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.json.JSONArray;

public class TestStoredProcedures
{
	 @Context
	 public GraphDatabaseService db;
	 
    // This rule starts a Neo4j instance
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the function we want to test
            .withProcedure( StoredProcedures.class );

    // @Test
    public void testValidateAddress() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();

            // When
            String result = "";
            StatementResult sResult;
            // result = session.run( "call horne.cdbg.sp.ValidateAddress(\"28623 Blue Holly Ln\", \"Katy\", \"TX\",\"77494\")").single().get("result").asString();
            sResult = session.run( "call horne.cdbg.sp.ValidateAddress(\"username\",\"28623 Blue Holly Ln\",\"\", \"Katy\", \"TX\",\"77494\")");
            result = sResult.single().toString();
            //System.out.println(name);
            System.out.println(result);
            
            // Then
            // assertThat( result, containsString( "OK"));
        }
    }
    
    // @Test
    public void testValidateAddressMelissa() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();

            // When
            String result = "";
            StatementResult sResult;
            // result = session.run( "call horne.cdbg.sp.ValidateAddress(\"28623 Blue Holly Ln\", \"Katy\", \"TX\",\"77494\")").single().get("result").asString();
            sResult = session.run( "call horne.cdbg.sp.ValidateAddressMelissa(\"username\",\"28623 Blue Holly Ln\",\"\", \"Katy\", \"TX\",\"77494\")");
            result = sResult.single().toString();
            //System.out.println(name);
            System.out.println(result);
            
            // Then
            // assertThat( result, containsString( "OK"));
        }
    }
    // @Test
    public void testDOB() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
        		//.driver( "bolt://localhost:7687",AuthTokens.basic("neo4j", "Qwerty123")) ) 
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();

            // When
            String result = "";
            StatementResult sResult;
            sResult = session.run( "call horne.cdbg.sp.DOB(\"username\",\"28623 Blue Holly Ln\",\"\", \"Katy\", \"TX\",\"77494\")");
            // sResult = session.run( "MATCH (a:State) RETURN a.name as StateName");
            result = sResult.single().toString();
//            List<Record> rList = sResult.list();
//            for (Record item : rList)
//            {
//            	result += item.toString();
//            }
            //result = sResult.first().toString();
            //System.out.println(name);
            System.out.println(result);
            
            // Then
            // assertThat( result, containsString( "OK"));
        }
    }
    
    
    // @Test
    public void testAudit() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
        		//.driver( "bolt://localhost:7687",AuthTokens.basic("neo4j", "Qwerty123")) ) 
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();

            // When
            String result = "";
            StatementResult sResult;
            
            System.out.println();
            System.out.println("TESTING horne.cdbg.sp.GetAuditFieldChanges:");
            sResult = session.run( "call horne.cdbg.sp.GetAuditFieldChanges('username', 'jhfhfhg','igggjhjh', '01/16/2017 22:55:55', '01/16/2017 23:55:55')");
          
            result = sResult.single().toString();
            List<Record> rList = sResult.list();
            for (Record item : rList)
            {
            	result += item.toString();
            }
            //result = sResult.first().toString();
            //System.out.println(name);
            System.out.println(result);
//           
//            System.out.println();
//            System.out.println("TESTING horne.cdbg.sp.GetAuditEvents:");
//            sResult = session.run( "call horne.cdbg.sp.GetAuditEvents('username', 'TX-FL16-01372', '01/16/2017 22:55:55', '01/16/2017 23:55:55')");
//            result = sResult.single().toString();
//            System.out.println(result);
            
//            System.out.println();
//            System.out.println("TESTING horne.cdbg.sp.GetAuditTasks:");
//            sResult = session.run( "call horne.cdbg.sp.GetAuditTasks('username', 'TX-FL16-01368', '01/16/2017 22:55:55', '01/16/2017 23:55:55')");
//            result = sResult.single().toString();
//            System.out.println(result); 
            
            // Then
            // assertThat( result, containsString( "OK"));
        }
    }
    
    // @Test
    public void testValidateAddressMelissaLoop() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
        		.driver( "bolt://localhost:7687",AuthTokens.basic("neo4j", "Qwerty123")) ) 
                //.driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();

            // When
            String result = "";
            StatementResult sResult;
            
            //System.out.println();
            System.out.println("TESTING: get all addresses unmatched by fuzzy search");
            sResult = session.run("match (a:ReferenceFEMA) \r\n" + 
            		"match (b:ReferenceFEMA)\r\n" + 
            		"where\r\n" + 
            		"id(a) <> a.LUCENE_R_ID\r\n" + 
            		"and\r\n" + 
            		"id(b) = a.LUCENE_R_ID\r\n" + 
            		"return\r\n" + 
            		"id(a) as id, "
            		+ "a.Damaged_Street as Damaged_Street,\r\n" + 
            		"a.Damaged_State as Damaged_State,\r\n" + 
            		"a.Damaged_City as Damaged_City,\r\n" + 
            		"coalesce(a.Zip,'') as Zip,\r\n" + 
            		"a.MD_Damaged_Street, b.MD_Damaged_Street,id(a), a.LUCENE_R_ID, a.LUCENE_weight limit 100");
            
            //result = sResult.single().toString();
            MelissaData logic = new MelissaData(db);
            List<Record> rList = sResult.list();
            for (Record item : rList)
            {
            	// result += item.toString();
            	System.out.println(item.get("Damaged_Street"));
            	String addrResult;
            	addrResult = logic.validateAddress(
            			item.get("Damaged_Street").toString(),
            			"",
            			item.get("Damaged_City").toString(),
            			item.get("Damaged_State").toString(),
            			item.get("Zip").toString()
            			);
            	JSONObject jsonObject = new JSONObject(addrResult);
            	JSONArray arRecords = jsonObject.getJSONArray("Records");
            	JSONObject jsonRecord = arRecords.getJSONObject(0);
            	System.out.println(jsonRecord.getString("AddressLine1"));
            	System.out.println(jsonRecord.getString("City"));
            	System.out.println(jsonRecord.getString("PostalCode"));
            	System.out.println(jsonRecord.getString("State"));
            	
            	Map<String, Object> vars = new HashMap<String, Object>();
            	vars.put("address1", jsonRecord.getString("AddressLine1"));
            	vars.put("id", item.get("id").asInt());
            	
            	StatementResult sResult2;
            	sResult2 = session.run("match (a:ReferenceFEMA)\r\n" + 
            			"where\r\n" + 
            			"id(a) = {id}\r\n" + 
            			"set a.MELISSA_Address1 = {address1} "
            			+ "return a", vars);
                System.out.println(sResult2.single().toString()); 
            	
            }
            //result = sResult.first().toString();
            //System.out.println(name);
//            System.out.println(result);
//           
//            System.out.println();
//            System.out.println("TESTING horne.cdbg.sp.GetAuditEvents:");
//            sResult = session.run( "call horne.cdbg.sp.GetAuditEvents('username', 'TX-FL16-01372', '01/16/2017 22:55:55', '01/16/2017 23:55:55')");
//            result = sResult.single().toString();
//            System.out.println(result);
            
        
            
            // Then
            // assertThat( result, containsString( "OK"));
        }
    }
   
    // @Test
    public void getProgramTypeList() throws Throwable
    {
        // This is in a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase
        		//.driver( "bolt://localhost:7687",AuthTokens.basic("neo4j", "Qwerty123")) ) 
                .driver( neo4j.boltURI() , Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() ) )
        {
            // Given
            Session session = driver.session();

            // When
            String result = "";
            StatementResult sResult;
            
            System.out.println();
          
            
            System.out.println();
            System.out.println("TESTING horne.cdbg.sp.GetProgramTypeList:");
            session.run("create (:ProgramType {name:'HUD CDBG - Homeowner Recovery Program'}) " +
            		"create (:ProgramType {name:'HUD CDBG - Rental Recovery Program'}) " +
            		"create (:ProgramType {name:'HUD CDBG - Long Term Workforce Housing Program'}) " +
            		"create (:ProgramType {name:'HUD CDBG - Small Rental Assistance Program'}) " +
            		"create (:ProgramType {name:'FEMA PA Program'}) " +
            		"create (:ProgramType {name:'FEMA HMGP Program'}) " +
            		"create (:ProgramType {name:'FEMA STEP Program'});");       
            
            sResult = session.run( "call horne.cdbg.sp.GetProgramTypeList()");
            
            
            result = sResult.single().toString();
            System.out.println(result); 
            
            // Then
            // assertThat( result, containsString( "OK"));
        }
    }
    
    
    // @Test
    public void getAuditFieldChangesTest() throws Throwable
    {
    	//String databaseDirectory = "EmbeddedDatabaseDirectory";
    	File databaseDirectory = new File("c:\\temp\\EmbeddedDatabase\\data\\databases\\graph.db");
    	GraphDatabaseService graphDb = new GraphDatabaseFactory()
    		    .newEmbeddedDatabaseBuilder( databaseDirectory )
    		    .loadPropertiesFromFile(  "c:\\temp\\EmbeddedDatabase\\conf\\" + "neo4j.conf" )
    		    .newGraphDatabase();
    	
    	//registerShutdownHook( graphDb );
    	
    	try ( Transaction tx = graphDb.beginTx() )
    	{
    		Result r2;
    		
    	/*	r2=graphDb.execute("call horne.cdbg.sp.GetAuditTasks()");
    		while(r2.hasNext())
    		{
    			Map<String, Object> row = r2.next();
        		System.out.println(row.toString());
    		}*/
    		
    		r2=graphDb.execute("call horne.cdbg.sp.GetAuditFieldChanges()");
    		while(r2.hasNext())
    		{
    			Map<String, Object> row = r2.next();
        		System.out.println(row.toString());
    		}
    		
    	/*	r2=graphDb.execute("call horne.cdbg.sp.GetAuditEvents()");
    		while(r2.hasNext())
    		{
    			Map<String, Object> row = r2.next();
        		System.out.println(row.toString());
    		}*/
    		
    		
    	    tx.success();
    	} // try
    	finally
    	{
    		graphDb.shutdown();
    	}
    	
    	
    /*	try
    	{
	    	StoredProceduresLogic logic = new StoredProceduresLogic(graphDb);
	    	//result = logic.getAuditFieldChanges(requestedby, casetextid, field, df.parse(datefrom), df.parse(dateto));
	    	String result = logic.getAuditFieldChanges("user", "TX-FL16-01368", "Status", "", "");
	    	System.out.println(result);
    	}
    	finally
    	{
    		graphDb.shutdown();
    	}*/
    	
    }

    
    
    
}

