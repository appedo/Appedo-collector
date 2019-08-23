package com.appedo.webcollector.webserver.manager;

import java.util.HashMap;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.SLABean;
import com.appedo.webcollector.webserver.manager.AWSJavaMail.MODULE_ID;

/**
 * This is a thread which is created for each SLA configuration.
 * This does the user required operation like sending EMail/SMS or calling a HTTP address.
 * 
 * @author Ramkumar R
 *
 */
public class SLAThread extends Thread {
	
	private SLABean slaBean = null;
	
	/**
	 * Create Thread object with the required SLA bean.
	 * 
	 * @param sla_bean
	 */
	public SLAThread(SLABean sla_bean) {
		slaBean = sla_bean;
	}
	
	/**
	 * Execute the user requested operation like sending EMail/SMS or calling a HTTP address.
	 */
	public void run() {
		String strSubject = "", strCounter = "";
		HashMap<String, Object> hmMailDetails = null;
		
		AWSJavaMail javaMail = null;
		
		try {
			strCounter = slaBean.getCounter();
			if( strCounter.equals("FREEPHYSICALMEMORY") ) {
				strCounter = "Free Physical Memory";
			}
			if( strCounter.equals("SYSTEMCPULOAD") ) {
				strCounter = "System CPU Load";
			}
			
			javaMail = AWSJavaMail.getManager();

			strSubject = "Appedo SLA: "+slaBean.getLevel().charAt(0)+slaBean.getLevel().substring(1,slaBean.getLevel().length()).toLowerCase()+" in "+strCounter;
			
			// Sends mail
			hmMailDetails = new HashMap<String, Object>();
			hmMailDetails.put("MODULE_NAME", slaBean.getModuleName()+" ("+slaBean.getModuleType()+")");
			hmMailDetails.put("LEVEL_INFO", slaBean.getLevel()+" level in "+strCounter);
			hmMailDetails.put("BREACHED_CONDITION", slaBean.getCounterValue()+" "+slaBean.getThresoldOperator() +" "+slaBean.getThresoldValue());
			hmMailDetails.put("NOTIFIED_BY_INFO", "Emails To: "+slaBean.getEmailIds() +"<BR/>SMS To:"+slaBean.getMobileNumbers());
			hmMailDetails.put("CAPA_INFO", slaBean.getCapaURL()+"<BR/>"+(slaBean.getCapaURL().length()>0?slaBean.getURLMethod():"")+"<BR/>"+slaBean.getURLParameters());
			hmMailDetails.put("BREACHED_ON", slaBean.getBreachedOn());
			
			javaMail.sendMail(MODULE_ID.SLA_EMAIL, hmMailDetails, slaBean.getEmailIds().split(","), strSubject);
			//System.out.println(slaBean.getSLAId()+" sending mail");
			
			// calls URL 
			if( slaBean.getCapaURL().length() > 0 ) {
				if ( slaBean.getURLMethod().equals("GET") ) {
					//System.out.println("GET - Runs URL : "+slaBean.getCapaURL());
					callURLUsingGetMethod(slaBean);
				} else if ( slaBean.getURLMethod().equals("POST") ) {
					//System.out.println("POST - Runs URL : "+slaBean.getCapaURL());
					callURLUsingPostMethod(slaBean);
				}
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
		} finally {
			
		}
	}
	
	/**
	 * Call a HTTP URL with the required parameters using Get Method.
	 * 
	 * @param slaBean
	 */
	private void callURLUsingGetMethod(SLABean slaBean) {
		HttpClient client = null;
		String responseJSONStream = null;
		
		String[] saParameter = null, strParamKeyValue = null;
		HttpMethodParams params = null;
		
		try {
			client = new HttpClient();
			
			// Process GET method 
			GetMethod method = new GetMethod(slaBean.getCapaURL());
			
			// Add parameters 
			if( slaBean.getURLParameters().length() > 0 ){
				saParameter = slaBean.getURLParameters().split("&");
				for (int i = 0; i < saParameter.length; i = i + 1 ) {
					strParamKeyValue = saParameter[i].split("=");
					
					params = new HttpMethodParams();
					params.setParameter(strParamKeyValue[0], strParamKeyValue[1]);
					method.setParams(params);
				}
			}
			
			// Send GET request
			int statusCode = client.executeMethod(method);
			System.err.println("statusCode: "+statusCode);
			
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			try {
				responseJSONStream = method.getResponseBodyAsString();
				//System.out.println("GetMethod  responseJSONStream : "+responseJSONStream);
				//System.out.println("Got response from the hit URL...");
			} catch (HttpException he) {
				LogManager.errorLog(he);
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
		}
	}
	
	/**
	 * Call a HTTP URL with the required parameters using Post Method.
	 * 
	 * @param slaBean
	 */
	private void callURLUsingPostMethod(SLABean slaBean) {
		HttpClient client = null;
		String responseJSONStream = null;
		
		String[] saParameter = null, strParamKeyValue = null;
		
		try {
			client = new HttpClient();
			
			// Process POST  method 
			PostMethod method = new PostMethod(slaBean.getCapaURL());
			
			// Adds parameters
			if( slaBean.getURLParameters().length() > 0 ){
				saParameter = slaBean.getURLParameters().split("&");
				for (int i = 0; i < saParameter.length; i = i + 1 ) {
					strParamKeyValue = saParameter[i].split("=");
					
					method.setParameter(strParamKeyValue[0], strParamKeyValue[1]);
				}
			}
			
			// Send POST request
			int statusCode = client.executeMethod(method);
			System.err.println("statusCode: "+statusCode);
			
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			try {
				responseJSONStream = method.getResponseBodyAsString();
				//System.out.println("PostMethod  responseJSONStream : "+responseJSONStream);
				//System.out.println("Got response from the hit URL...");
			} catch (HttpException he) {
				LogManager.errorLog(he);
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
		}finally {
			client = null;
			responseJSONStream = null;		
			saParameter = null;
			strParamKeyValue = null;			
		}
	}
}
