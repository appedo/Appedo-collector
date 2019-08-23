package com.appedo.webcollector.webserver.resource;

import java.sql.Connection;

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
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;

public class FileComparison extends Resource{

	
	public FileComparison(Context context, Request request, Response response) {
		super(context, request, response);
		//System.out.println("collectorResource: "+request.getEntity().toString());
		
		// Declare the kind of representations supported by this resource.
		getVariants().add(new Variant(MediaType.MULTIPART_FORM_DATA));

		// Allow modifications of this resource via POST requests.
		setModifiable(true);
		
		
	}
	
	@Override
	/**
	 * Handle POST requests: Receive the Counter data with agent_type
	 * 
	 * @param entity Form entity
	 * @throws ResourceException
	 */
	public void acceptRepresentation(Representation entity) throws ResourceException {
		
		StringBuilder responseXml = null;
	
		Form frm = new Form(entity);	
		
		// get data from the request
		//System.out.println(frm.getFirstValue("dataBytes"));
		
		byte[] dataBytes = frm.getFirstValue("dataBytes").getBytes();
		
		String GUID = frm.getFirstValue("GUID");
		
		long installed_app_on = Long.parseLong(frm.getFirstValue("installed_app_on"));
		
		String strCommand = UtilsFactory.replaceNull(frm.getFirstValue("command"), "");
		
		switch(strCommand) {
		
			case "fileCompare" : {
				String fileName = frm.getFirstValue("fileName");
				responseXml = fileCompareResponseMessage(dataBytes, GUID, installed_app_on, fileName, strCommand);
				break;
			}
			
			case "installingAppList" : {
				responseXml = collectorResponseMessage(dataBytes, GUID, installed_app_on, strCommand);
				break;
			}
		}
		
		/*try {
			responseXml = collectorResponseMessage(dataBytes, GUID, installed_app_on);
		}catch(Exception e) {
			LogManager.errorLog(e);
		}*/
		
	
		Representation rep = new StringRepresentation(responseXml);
		getResponse().setEntity(rep);
	}
	
	
	/**
	 * get dataBytes from agent and write new file and upload a Amazon S3.
	 * 
	 * @param uid , dataBytes
	 * @return
	 */
	private StringBuilder collectorResponseMessage(byte[] dataBytes, String GUID, long installed_app_on, String command) {
		StringBuilder sbXML = null;
		
		CollectorManager collectorManager = null;
		Connection con = null;
		
		try {
			//System.err.println(System.currentTimeMillis());
			con = DataBaseManager.giveConnection();
			
			collectorManager = CollectorManager.getCollectorManager();
		
			if(Constants.IS_ONPREM) {
				collectorManager.sendModuleService(dataBytes, GUID, installed_app_on, command, null);
			}else {
				collectorManager.WriteAndUploadFileOnInstallAppList(con, dataBytes, GUID, installed_app_on);
			}
			
				
			sbXML = UtilsFactory.getJSONSuccessReturn("Successfully File Uploaded");
			System.err.println(System.currentTimeMillis());	
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
			//throw e;
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		System.out.println("Response :" +sbXML.toString());
		return sbXML;
	}
	
	/**
	 * get dataBytes from agent and write new file and upload a Amazon S3.
	 * 
	 * @param uid , dataBytes
	 * @return
	 */
	private StringBuilder fileCompareResponseMessage(byte[] dataBytes, String GUID, long installed_app_on, String fileName, String command) {
		StringBuilder sbXML = null;
		
		CollectorManager collectorManager = null;
		Connection con = null;
		
		try {
			//System.err.println(System.currentTimeMillis());
			con = DataBaseManager.giveConnection();
			
			collectorManager = CollectorManager.getCollectorManager();
		
			if(Constants.IS_ONPREM) {
				collectorManager.sendModuleService(dataBytes, GUID, installed_app_on, command, fileName);
			}else {
				collectorManager.WriteAndUploadFileOnFileCompare(con, dataBytes, GUID, installed_app_on, fileName);
			}
			sbXML = UtilsFactory.getJSONSuccessReturn("Successfully File Uploaded");
			
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
			//throw e;
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		System.out.println("Response :" +sbXML.toString());
		return sbXML;
	}
	
	/**
	 * main function to test this resource
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		
		//logger.debug("Entered into : getUser: getDocForRequest "+ requestUrl);
		/*HttpClient client = null;
		String responseJSONStream = null;
		
		try{
			client = new HttpClient();
			// URLEncoder.encode(requestUrl,"UTF-8");
			//PostMethod method = new PostMethod("http://localhost:8080/SS_Profiler/collectCounters");
			PostMethod method = new PostMethod("http://localhost:8080/Appedo-Collector/collectCountersV2");
			
	
			//method.setParameter("agent_type", "MYSQL");
			int statusCode = client.executeMethod(method);
			System.out.println("statusCode: "+statusCode);
			
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
		} finally {
			client = null;
			responseJSONStream = null;
		}*/
	}

}
