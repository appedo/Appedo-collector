package com.appedo.webcollector.webserver.resource;

import java.io.IOException;
import java.util.Date;

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

import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.DotNetProfilerManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants.AGENT_TYPE;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * Performance Counter Collector Resource
 * This service will receive all the counter data and pass it to respective Manager.
 * 
 * @author Ramkumar
 *
 */
public class ProfilerCollectorResource extends Resource {
	
	public ProfilerCollectorResource(Context context, Request request, Response response) {
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
		Form frm = new Form(entity);
		
		// Get data from the request
		AGENT_TYPE agent_type = AGENT_TYPE.valueOf(frm.getFirstValue("agent_type"));
		String strGUID = frm.getFirstValue("guid");
		
		// JAVA Profiler returns JSONArray. But DotNet returns CSV
		String strProfilerArray = null, strMethodsCSV = null;
		
		// TODO Ram; merge below two if-else
		
		if( agent_type == AGENT_TYPE.JAVA_PROFILER ) {
			strProfilerArray = frm.getFirstValue("profiler_array_json");
		} else if( agent_type == AGENT_TYPE.DOTNET_PROFILER ) {
			strMethodsCSV = frm.getFirstValue("profiler_data_csv");
		}
		
		// Call the manager function
		if( agent_type == AGENT_TYPE.DOTNET_PROFILER ){
			// Handle no date & Older MSIIS Profiler request.
			if( strMethodsCSV == null ) {
				responseXml = UtilsFactory.getJSONFailureReturn("No data sent. Please try again after exploring your application.");
			} else if( frm.getFirstValue("profiler_array_json") != null && frm.getFirstValue("profiler_array_json").length() > 0 && strMethodsCSV.length() == 0 ) {
				LogManager.errorLog("Appedo-Profiler is older version. Please upgrade the agent. GUID: "+strGUID);
				responseXml = UtilsFactory.getJSONFailureReturn("Appedo-Profiler is older version. Please upgrade the agent.");
			} else {
				responseXml = collectProfilerCSV(agent_type, strGUID, strMethodsCSV);
			}
		} else {
			responseXml = collectProfilerArray(agent_type, strGUID, strProfilerArray);
		}
		
		Representation rep = new StringRepresentation(responseXml);
		getResponse().setEntity(rep);
		
		agent_type = null;
		strGUID = null;
		strProfilerArray = null;
	}
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private StringBuilder collectProfilerArray(AGENT_TYPE agent_type, String strGUID, String strProfilerArray) {
		StringBuilder sbXML = null;
		boolean bQueued = false;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			
			if( agent_type == AGENT_TYPE.JAVA_PROFILER ) {
				bQueued = collectorManager.collectJavaProfiler(strGUID, "{1001: \""+strGUID+"\", profilerArray: "+strProfilerArray+"}");
//			} else if( agent_type == AGENT_TYPE.DOTNET_PROFILER ) {
//				bQueued = collectorManager.collectDotNetProfiler("{1001: \""+strGUID+"\", profilerArray: "+strProfilerArray+"}");
			} else if(agent_type == AGENT_TYPE.POSTGRES ){
				bQueued = collectorManager.collectPGSlowQueries(strGUID, "{1001: \""+strGUID+"\", profilerArray: "+strProfilerArray+"}");
			}
			
			if( bQueued ){
				sbXML = UtilsFactory.getJSONSuccessReturn("Data queued.");
			} else {
				sbXML = UtilsFactory.getJSONFailureReturn("Unable to queue data. Try again later.");
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		
		return sbXML;
	}
	
	/**
	 * Insert the DotNet Profiler data into DB directly, without queuing.
	 * If it is queued (or) received in ELB then, DB order will get changed.
	 * So Stack re-structure will be difficult one.
	 * 
	 * @param agent_type
	 * @param strGUID
	 * @param strTimerThreadId
	 * @param strMethodsCSV
	 * @return
	 */
	private StringBuilder collectProfilerCSV(AGENT_TYPE agent_type, String strGUID, String strMethodsCSV) {
		StringBuilder sbXML = null;
		Long lInserted = null;
		Date dateLog = LogManager.logMethodStart();
		
		DotNetProfilerManager dotNetProfilerManager = null;
		
		try {
			dotNetProfilerManager = new DotNetProfilerManager();
			
			dotNetProfilerManager.establishDBConnection();
			
			lInserted = dotNetProfilerManager.insertMethodDetails(strGUID, strMethodsCSV);
			
			if( lInserted != 0 ){
				sbXML = UtilsFactory.getJSONSuccessReturn("Data saved.");
			} else {
				sbXML = UtilsFactory.getJSONFailureReturn("Unable to save data. Try again later.");
			}
			
			dotNetProfilerManager.commitConnection();
		} catch (Throwable th) {
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( th.getMessage() );
			LogManager.errorLog(th);
		} finally {
			dotNetProfilerManager.closeConnection();
		}
		
		LogManager.logMethodEnd(dateLog);
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
			PostMethod method = new PostMethod("http://localhost:80/appedo_collector/collectProfilerStack");
			
			method.setParameter("guid", "86561a3f-62e6-45da-9d1c-9b3c0c8e99de");
			method.setParameter("agent_type", "DOTNET_PROFILER");
			method.setParameter("profiler_array_json", "[{1=635567693547085763,3=\"2015-01-13 12:39:14\",4=15,51=\"ASP.login_aspx\",52=\"FrameworkInitialize\",53=\"\",54=-1,55=1,7=\"ASP.login_aspx\"},{1=635567693547085763,3=\"2015-01-13 12:39:14\",4=15,51=\"ASP.login_aspx\",52=\"FrameworkInitialize\",53=\"\",54=-1,55=1,7=\"ASP.login_aspx\"}]");
			
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
