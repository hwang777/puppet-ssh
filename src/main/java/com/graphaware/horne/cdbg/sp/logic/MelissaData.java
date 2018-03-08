package com.graphaware.horne.cdbg.sp.logic;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import javax.xml.transform.TransformerException;

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


public class MelissaData {
	
	
	private final GraphDatabaseService database;
	
	@Context
	public GraphDatabaseService db;
	
	@Context
	public Log log;
	
	//Web service URL
	private String httpAddress = "//personator.melissadata.net/v3/WEB/ContactVerify/doContactVerify?";
	
	//Strings for input data
	private String custID = "nAN19rAw43y30sHUBMT12c**";
	private String actions;
	private String transmissionReference;
	private String columns;
	private String options;
	private String recordID;
	
	private String address1;
	private String address2;
	private String city;
	private String state;
	private String postalCode;
	private String country = "USA";
	
	
	//The elements needed for the frame
	private String[] CentricOptions = new String[4];
	private String[] AACOptions = new String[2];
	private String[] AppOptions = new String[3];


	
	public MelissaData(GraphDatabaseService database) {
        this.database = database;
    	CentricOptions[0] = "Auto";
		CentricOptions[1] = "Address";
		CentricOptions[2] = "Phone";
		CentricOptions[3] = "Email";
		
		AACOptions[0] = "Off";
		AACOptions[1] = "On";
		
		AppOptions[0] = "Blank";
		AppOptions[1] = "CheckError";
		AppOptions[2] = "Always";
    }
	

	public String validateAddress(
			String aAddress1,
			String aAddress2,
			String aCity,
			String aState,
			String aPostalCode
			)
	{
		address1 = aAddress1;
		address2 = aAddress2;
		city = aCity;
		state = aState;
		postalCode = aPostalCode;
		
		String response = "";
		
		String request = null;
		try {
			request = createRESTRequest();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		
		try {
			response = sendRequest(request);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		
		return response;
	}
	
	//Creates the URL used for the REST request based on the input in the forms
	private String createRESTRequest() throws TransformerException
	{
		transmissionReference = "12345";
		
		actions = "";
		
		if(true)
		{
			actions = "Check";
		}
		if(true)
		{
			actions += ";Verify";
		}
		if(true)
		{
			actions += ";Append";
		}
		if(true)
		{
			actions += ";Move";
		}
		
		options = "";
		
		options += "CentricHint:";
		options += CentricOptions[0];
		
		options += ";AdvancedAddressCorrection:";
		options += AACOptions[1];
		
		options += ";Append:";
		options += AppOptions[2];
		
		columns = "";
		if(true)
		{
			columns += "GrpNameDetails,";
		}
		if(true)
		{
			columns += "GrpParsedAddress,";
		}
		if(true)
		{
			columns += "GrpAddressDetails,";
		}
		if(true)
		{
			columns += "GrpParsedEmail,";
		}
		if(true)
		{
			columns += "GrpParsedPhone,";
		}
		if(true)
		{
			columns += "GrpCensus,";
		}
		if(true)
		{
			columns += "GrpGeocode,";
		}
		
		String rest = "t=" + transmissionReference + "&" 
					+ "id=" + custID + "&"
					+ "act=" + actions + "&"
					+ "cols=" + columns + "&"
					+ "opt=" + options + "&"
					+ "first=&"
					+ "last=&"
					//+ "full=" + tFullName.getText() + "&"
					//+ "comp=" + tCompanyName.getText() + "&"
					+ "a1=" + address1 + "&"
					+ "a2=" + address2 + "&"
					+ "city=" + city+ "&"
					+ "state=" + state + "&"
					+ "postal=" + postalCode + "&"
					+ "ctry=" + country + "&"
					//+ "lastlines=" + tLastLine.getText() + "&"
					+ "freeform=&"
					//+ "email=" + tEmail.getText() + "&"
					//+ "phone=" + tPhone.getText() + "&"
					+ "format=JSON&reserved=";
		
		return rest;
	}
	
	
	private String sendRequest(String restRequest) throws IOException
	{
		//puts together the whole URL
		URI uri = null;
		try {
			uri = new URI("https", httpAddress + restRequest, null);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		
		URL url = new URL(uri.toURL().toString());
		//System.out.println(url.toString());
		
		//Open a connection
		HttpURLConnection urlConn = (HttpURLConnection)(url.openConnection());
		
		//Read the XML response and write it into a file
		BufferedReader in = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
		
		String inputLine = "";
		String xmlString = "";
		
		
		while((inputLine = in.readLine()) != null)
		{
			xmlString += inputLine;
			
//			try
//			{
//				FileWriter newFile = new FileWriter("XMLResponse.xml");
//				newFile.write(xmlString);
//				newFile.flush();
//				newFile.close();
//			}
//			catch(IOException ioe)
//			{
//				System.out.println(ioe);
//			}
		}
		
		return xmlString;
		
		//outputResults();
	}
	

	
} // class
