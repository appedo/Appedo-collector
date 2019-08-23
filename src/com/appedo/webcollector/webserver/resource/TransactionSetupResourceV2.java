
package com.appedo.webcollector.webserver.resource;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;


public class TransactionSetupResourceV2 extends Resource {
	
	public TransactionSetupResourceV2(Context context, Request request, Response response) {
		super(context, request, response);

		// Declare the kind of representations supported by this resource.
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		// Allow modifications of this resource via POST requests.
		setModifiable(true);
	}

	@Override
	/**
	 * Handle POST requests: Recieve the Counter data with agent_type
	 * 
	 * @param entity
	 *			Form entity
	 * @throws ResourceException
	 */
	public void acceptRepresentation(Representation entity) throws ResourceException {
		StringBuilder responseXml = null;
		Connection con = null;
		
		try{
			Form frm = new Form(entity);
			
			// get data from the request
			String strGUID = frm.getFirstValue("guid");
			//String strAgentType = UtilsFactory.replaceNull(frm.getFirstValue("agent_type"), "");;
			//String strnewCounterSet = frm.getFirstValue("CounterSet");
			String strCommand = UtilsFactory.replaceNull(frm.getFirstValue("command"), "");
			String strUUID = frm.getFirstValue("uuid");
			
			switch(strCommand) {
								
				case "UnifiedAgentFirstRequest" : {
					con = DataBaseManager.giveConnection();
					
					responseXml = getConfigGUID(con, strUUID);
					
					break;
				}
				
				case "getUIDStatus" :{
					con = DataBaseManager.giveConnection();
					
					responseXml = getAgentStatus(con, strGUID);
					
					break;
				}
				
				case "NETSTACK" :{
					String strEid = frm.getFirstValue("eid");
					
					con = DataBaseManager.giveConnection();
					
					responseXml = getProfilerConfigGUID(con, strUUID, strEid);
					
					break;
				}
				
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			responseXml = new StringBuilder(UtilsFactory.getJSONFailureReturn( e.getMessage() ));
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		Representation rep = new StringRepresentation(responseXml);
		getResponse().setEntity(rep);
	}
	
	
	/**
	 * Process edit profile headers
	 * @param userid
	 * @return
	 */
	private StringBuilder getConfigGUID(Connection con, String strUUID) {
		HashMap<String, Object> hmKeyValues = new HashMap<String, Object>();
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;

		try {		
			collectorManager = CollectorManager.getCollectorManager();
			hmKeyValues = collectorManager.getModuleConfigCountersTest(con, strUUID);

			sbXML = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
			LogManager.infoLog(sbXML.toString());
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		
		return sbXML;
	}
	
	private StringBuilder getProfilerConfigGUID(Connection con, String strUUID, String strEid) {
		HashMap<String, Object> hmKeyValues = new HashMap<String, Object>();
		StringBuilder sbXML = null;
		String strResponse = null;
		CollectorManager collectorManager = null;

		try {		
			collectorManager = CollectorManager.getCollectorManager();
						
			strResponse = collectorManager.getProfilerModuleConfigGuid(con, strUUID, strEid);
	
			sbXML = new StringBuilder(strResponse);
			
			LogManager.infoLog(sbXML.toString());
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		
		return sbXML;
	}
	
	private StringBuilder getAgentStatus(Connection con, String strGUID){
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		try {
			collectorManager = CollectorManager.getCollectorManager();
			sbXML = collectorManager.getModuleStatus(con, strGUID);
			LogManager.infoLog("Response of "+strGUID+" status: "+sbXML.toString());
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		return sbXML;
	}
	
	/**
	 * main function to test this resource
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		
		//logger.debug("Entered into : getUser: getDocForRequest "+ requestUrl);
		HttpClient client = null;
		String responseJSONStream = null;
		
		try{
			client = new HttpClient();
			// URLEncoder.encode(requestUrl,"UTF-8");
			//PostMethod method = new PostMethod("http://localhost:8080/SS_Monitor_Webservice/firstRequest");
			PostMethod method = new PostMethod("http://localhost:8080/Appedo-Collector/getConfigurationsV2");
			
			//method.setParameter("agent_type", "MYSQL");
			//method.setParameter("guid", "2b394f93-c065-4d70-a8c6-cbfd9eb27386");
			
			
			//method.setParameter("command", "UnifiedAgentFirstRequest");
			//method.setParameter("uuid", "4C4C4544-0053-4610-8035-B9C04F433132");
			method.setParameter("uuid", "4C4C4544-0034-4410-804E-B8C04F363332");
			//method.setParameter("uuid", "4C4C4544-004C-5610-804A-B2C04F323332");
			method.setParameter("eid", "7");
			
			//Request for module status
			method.setParameter("command", "NETSTACK");
			//method.setParameter("guid", "99af690c-8bc1-4225-88cb-042af918cada");
			
			int statusCode = client.executeMethod(method);
			System.err.println("statusCode: "+statusCode);
			
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			try {
				responseJSONStream = method.getResponseBodyAsString();
				System.out.println("responseJSONStream: "+responseJSONStream);
			} catch (HttpException he) {
				LogManager.errorLog(he);
			}
		} catch (IOException ie) {
			LogManager.errorLog(ie);
		} catch (Exception e) {
			LogManager.errorLog(e);
		}finally {
			client = null;
			responseJSONStream = null;
		}
	}

}
