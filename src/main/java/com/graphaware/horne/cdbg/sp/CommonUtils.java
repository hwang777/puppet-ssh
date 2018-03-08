package com.graphaware.horne.cdbg.sp;

import java.util.Map;
import java.util.HashMap;

import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Node;

public class CommonUtils {
	
	// This checks if the node id is null and returns a null node
	public Node checkForNullNode (GraphDatabaseService db, Object value) 
	{
		if(value == null)
			return null;
		else
			return db.getNodeById((long) value);
	}
		
	// This checks if the property exists on the node and returns it; empty string otherwise
	public String getProperty (Node node, String property)
	{
		if(node == null)
			return "";
		else {
			if(property.toUpperCase().equals("ID"))
				return new Long(node.getId()).toString();
			else if(node.hasProperty(property))
				return node.getProperty(property).toString();
			else
				return "";
		}
	}
	
	// This checks if the field has a non-null value in the row and returns it; empty string otherwise
	public String getRowValueString (Map<String, Object> row, String property)
	{
		if(row.containsKey(property)) {
			if(row.get(property) == null) 
				return "";
			else
				return row.get(property).toString();
		} else
			return "";
	}
	
	// This checks if the field has a non-null value in the row and returns it; empty string otherwise
	public Long getRowValueLong (Map<String, Object> row, String property)
	{
		if(row.containsKey(property)) {
			if(row.get(property) == null) 
				return new Long(0);
			else
				return new Long(row.get(property).toString());
		} else
			return new Long(0);
	}
	
	// This checks if the property exists in the JSON object and returns it; empty string otherwise
	public String getJSONStringProperty (JSONObject json, String property)
	{
		if(json.has(property)) 
			return json.getString(property);
		else
			return "";
	}
	
	// This checks if the property exists in the JSON object and returns it; empty string otherwise
	public Long getJSONLongProperty (JSONObject json, String property)
	{
		if(json.has(property)) 
			return json.getLong(property);
		else
			return new Long(0);
	}

    // Looks up the CypherQuery node that matches the name input parameter and returns the cypher
    public String getCypherQuery (GraphDatabaseService db, String name) throws Exception {
    	String cypher = "";
	
		// Set up the parameters
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("name", name);
			
		Result result = db.execute("MATCH (n:CypherQuery) WHERE n.name={name} RETURN n.query AS query", params);
		if(!result.hasNext()) throw new Exception("The cypher query was not found");
		
		while(result.hasNext())
		{
			Map<String, Object> row = result.next();

			cypher = row.get("query").toString();
		}

		return cypher;
    }

}
