package com.graphaware.horne.cdbg.sp;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;

import com.graphaware.horne.cdbg.sp.Tasks;

import jline.internal.Log;


public class TasksTest {
	
	// This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(Tasks.class);
	
	@Test
	public void shouldAllowUpdatingTask() throws Throwable
	{
		try(Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()))
		{
			Session session = driver.session();
			
			// Add the cypher query
			session.run("CREATE (new:CypherQuery {name:\"UPDATE_TASK_QUERY\", query:\"MATCH (casePhase:CasePhase) where id(casePhase) = {CasePhaseId}  " + 
					"   Merge (casePhase)-[:HAS_TASK]->(task:Task{AppianTaskId:{AppianTaskId}})  " + 
					"   set task.Name ={TaskName}, task.AppianTaskStatus ={AppianTaskStatus}, task.AppianTaskAssignee = {AppianTaskAssignee},  " + 
					"   task.City = {TaskCity}, task.County = {TaskCounty}  " + 
					"   return id(task) as TaskId;\"}) RETURN id(new);");
		
			// Add the supporting metadata
			Long casePhaseId = session.run("CREATE (n:CasePhase) RETURN id(n) AS CasePhaseId")
					.single()
					.get(0).asLong();
			
			// Run the test
			String json = session.run("CALL horne.cdbg.sp.updateTask(\"Test Harness\", " + casePhaseId.toString() + ", \"12345\", \"Test Task\", \"Started\", \"JDoe\", \"Austin\", \"Travis\")")
					.single()
					.get(0).asString();
			Log.info("json: %s",json);
			
			// Extract the result from the json
			JSONObject resultObj = new JSONObject(json);
			assertThat(resultObj.get("success").toString(), equalTo("true"));
			
			JSONArray  resultArr = new JSONArray(resultObj.get("result").toString());
			Long taskId = resultArr.getJSONObject(0).getLong("taskId");
		
			// Check the result
			StatementResult result = session.run("MATCH (task:Task) WHERE id(task) = " + taskId.toString() + " RETURN task.AppianTaskAssignee AS AppianTaskAssigneee");
			assertThat(result.single().get(0).asString(), equalTo("JDoe"));
		}
	}

}
