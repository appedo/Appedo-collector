
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
import com.appedo.webcollector.webserver.manager.AWSJavaMail;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * Performance Counter Collector Resource
 * This service will receive all the counter data and pass it to respective Manager.
 * 
 * @author Ramkumar
 *
 */
public class CommonResource extends Resource {
	
	public CommonResource(Context context, Request request, Response response) {
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
	 * Handle POST requests: Receive the Counter data with agent_type
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
			// String strGUID = frm.getFirstValue("guid");
			
			
			// Loads Constant properties 
			Constants.loadConstantsProperties(Constants.CONSTANTS_FILE_PATH);
			
			con = DataBaseManager.giveConnection();
			
			// loads Appedo constants: WhiteLabels, Config-Properties
			Constants.loadAppedoConstants(con);
			
			// Loads Appedo config properties from the system path
			Constants.loadAppedoConfigProperties(Constants.APPEDO_CONFIG_FILE_PATH);
			
			// Loads mail config
			AWSJavaMail.getManager().loadPropertyFileConstants(Constants.SMTP_MAIL_CONFIG_FILE_PATH);
			
			responseXml = new StringBuilder("Loaded <B>Appedo-LOG-Collector</B>, config, appedo_config & appedo whitelabels.");
			
		} catch (Throwable th) {
			LogManager.errorLog(th);
			
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			responseXml = new StringBuilder("<B style=\"color: red; \">Exception occurred Appedo-LOG-Collector: "+th.getMessage()+"</B>");
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		Representation rep = new StringRepresentation(responseXml);
		getResponse().setEntity(rep);
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
