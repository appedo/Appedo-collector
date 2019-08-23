
package com.appedo.webcollector.webserver.resource;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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

/**
 * Performance Counter Collector Resource
 * This service will receive all the counter data and pass it to respective Manager.
 * 
 * @author Ramkumar
 *
 */
public class TransactionSetupResource extends Resource {
	
	public TransactionSetupResource(Context context, Request request, Response response) {
		super(context, request, response);

		// Declare the kind of representations supported by this resource.
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		// Allow modifications of this resource via POST requests.
		setModifiable(true);
	}
	
//	public Representation represent(Variant variant) throws ResourceException {
//		StringRepresentation srRet = null;
//		String strGUID = "", strCommand = "";
//
//		try {
//			strGUID = (String) getRequest().getAttributes().get("guid");
//			strCommand = (String) getRequest().getAttributes().get("command");
//			
//			queueNewMonitorCounterSet(strGUID);
//			
//			srRet = new StringRepresentation( UtilsFactory.getJSONSuccessReturn("Successfully updated."));
//		  }catch(Exception e){
//			  
//		  }
//		
//		return srRet;
//	}
	
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
			String strAgentType = UtilsFactory.replaceNull(frm.getFirstValue("agent_type"), "");;
			String strnewCounterSet = frm.getFirstValue("CounterSet");
			String strCommand = UtilsFactory.replaceNull(frm.getFirstValue("command"), "");
			
			switch(strCommand) {
				case "MonitorCounterSet" : {
					setConfigurations(strGUID, strnewCounterSet);
					break;
				}
				
				case "updateMonitorCounterSet" : {
					con = DataBaseManager.giveConnection();
					
					queueNewMonitorCounterSet(con, strGUID);
					
					break;
				}
				
				case "SlaCounterSet" : {
					// Note: `strGUID` has multiple guid(s), `strnewCounterSet` is GUID wise new SLA(s)
					setSlaConfigurations(strGUID, strnewCounterSet);
					break;
				}
				
				case "AgentFirstRequest" : {
					con = DataBaseManager.giveConnection();
					
					responseXml = getConfigCounters(con, strGUID);
					
					break;
				}
				
				case "GetCounterSet" : {
					strAgentType = "";
					
					con = DataBaseManager.giveConnection();
					
					responseXml = getConfigurations(con, strGUID, strAgentType);
					
					break;
				}
				
				case "ChildCounterSet" : {
					JSONObject joRecChildCounters = JSONObject.fromObject(frm.getFirstValue("CounterSet"));
					
					con = DataBaseManager.giveConnection();
					
					responseXml = configChildCounters(con, strGUID, joRecChildCounters);
					
					break;
				}
				
				default  : {
					con = DataBaseManager.giveConnection();
					
					responseXml = getConfigurations(con, strGUID, strAgentType);
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
	 * 
	 * @param userid
	 * @return
	 */
	private StringBuilder configChildCounters(Connection con, String strGUID, JSONObject joRecChildCounters) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			
			sbXML = collectorManager.updateChildCounters(con, strGUID, joRecChildCounters);
				//System.out.println("pqModulePerformanceCounters: "+sbXML);
			
			
			if( sbXML == null ){
				sbXML = UtilsFactory.getJSONFailureReturn("Unable to update child counters. Try again later.");
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		return sbXML;
	}
	
	/**
	 * Process edit profile headers
	 * @param userid
	 * @return
	 */
	private StringBuilder getConfigCounters(Connection con, String strGUID) {
		HashMap<String, Object> hmKeyValues = new HashMap<String, Object>();
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			hmKeyValues = collectorManager.getModuleConfigCounters(con, strGUID);
			
			if( hmKeyValues.containsKey("KILL") ) {
				sbXML = UtilsFactory.getJSONSuccessReturn("kill");
			} else if( hmKeyValues.size() > 0 ) {
				sbXML = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
			} else {
				sbXML = UtilsFactory.getJSONFailureReturn("Unable to get UID. Try again later.");
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		
		return sbXML;
	}
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private StringBuilder getConfigurations(Connection con, String strGUID, String strAgentType) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			
			if ( strAgentType.equals("JAVA_PROFILER") ) {
				// send the MS IIS counter data to MySQLCounter Manager.
				sbXML = collectorManager.getProfilerConfigurations(con, strGUID);
				//System.out.println("pqJavaProfilerPerformanceCounters: "+sbXML);
			}else {
				LogManager.infoLog("Inside getConfigurations");
				sbXML = collectorManager.getModuleConfigurations(strGUID);
				//System.out.println("pqModulePerformanceCounters: "+sbXML);
			}
			
			if( sbXML == null ){
				sbXML = UtilsFactory.getJSONFailureReturn("Unable to get UID. Try again later.");
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		return sbXML;
	}
	
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private void setConfigurations(String strGUID, String strnewCounterSet) {
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			collectorManager.setModuleConfigurations(strGUID, strnewCounterSet);			
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
		}
	}
	
	private void queueNewMonitorCounterSet(Connection con, String strGUID) throws Exception {
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			collectorManager.queueNewMonitorCounterSet(con, strGUID);
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
		}
	}
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private void setSlaConfigurations(String strGUIDs, String strGUIDsNewSLAsCounters) {
		CollectorManager collectorManager = null;
		
		String[] saGUIDs = null;
		String strGUID = "";
		
		JSONObject joGUIDsNewSLAs = null;
		JSONArray jaGUIDNewSLAs = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			
			joGUIDsNewSLAs = JSONObject.fromObject(strGUIDsNewSLAsCounters);
			
			// sets 
			saGUIDs = strGUIDs.split(",");
			for(int i = 0; i < saGUIDs.length; i = i + 1) {
				strGUID = saGUIDs[i];
				
				jaGUIDNewSLAs = joGUIDsNewSLAs.getJSONArray(strGUID);
				
				collectorManager.setSlaModuleConfigurations(strGUID, jaGUIDNewSLAs);
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
		} finally {
			saGUIDs = null;
			strGUID = null;
		}
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
			PostMethod method = new PostMethod("http://localhost:8080/SS_Monitor_Webservice/firstRequest");
			
			method.setParameter("agent_type", "MYSQL");
			method.setParameter("guid", "2b394f93-c065-4d70-a8c6-cbfd9eb27386");
			
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
