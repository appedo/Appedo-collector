package com.appedo.webcollector.webserver.resource;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;

import net.sf.json.JSONArray;

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
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.ModulePerformanceCounterManager;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;
import com.appedo.webcollector.webserver.util.Constants.AGENT_TYPE;

/**
 * Performance Counter Collector Resource
 * This service will receive all the counter data and pass it to respective Manager.
 * 
 * @author Ramkumar
 *
 */
public class PCCollectorResource extends Resource {
	
	public PCCollectorResource(Context context, Request request, Response response) {
		super(context, request, response);

		// Declare the kind of representations supported by this resource.
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		// Allow modifications of this resource via POST requests.
		setModifiable(true);
	}
	
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
		
		Form frm = new Form(entity);	
		// get data from the request
		String strAgentType = frm.getFirstValue("agent_type");
		
		String strCounterParams = frm.getFirstValue("counter_params_json");
		
		String strGUID = frm.getFirstValue("guid");
		
		String strCommand = UtilsFactory.replaceNull(frm.getFirstValue("command"), "");
		String strSlaBreachCounterset = frm.getFirstValue("breach_counter_set");
		
		switch(strCommand) {
			
			case "sla" : {
				responseXml = sendToSlaCollector(strAgentType, strSlaBreachCounterset, strGUID );
				break;
			}
			case "slowqry" : {
				AGENT_TYPE agent_type = AGENT_TYPE.valueOf(frm.getFirstValue("agent_type"));
				String strTimerThreadId = frm.getFirstValue("timer_thread_id");
				//String strProfilerArray =makeValidVarchar(frm.getFirstValue("profiler_array_json"));
				String strProfilerArray = frm.getFirstValue("profiler_array_json");
				
				responseXml = collectProfilerArray(agent_type, strGUID, strTimerThreadId, strProfilerArray);
				
				agent_type = null;
				strGUID = null;
				strTimerThreadId = null;
				strProfilerArray = null;
				break;
			}
			case "slowprocedure" : {
				
				AGENT_TYPE agent_type = AGENT_TYPE.valueOf(frm.getFirstValue("agent_type"));
				String strTimerThreadId = frm.getFirstValue("timer_thread_id");
				//String strProfilerArray =makeValidVarchar(frm.getFirstValue("profiler_array_json"));
				String strProfilerArray =frm.getFirstValue("profiler_array_json");
				
				responseXml = collectProfilerArrayForProcedure(agent_type, strGUID, strTimerThreadId, strProfilerArray);
				
				agent_type = null;
				strGUID = null;
				strTimerThreadId = null;
				strProfilerArray = null;
				break;
			}
			case "beat" : {
				AGENT_TYPE agent_type = AGENT_TYPE.valueOf(frm.getFirstValue("agent_type"));
				responseXml = collectBeatMessage(strGUID);
			}
			default  : {
				responseXml = collectMessage(strAgentType, strCounterParams, strGUID);
				//break;
			}
		}
		
		Representation rep = new StringRepresentation(responseXml);
		getResponse().setEntity(rep);
	}
	
	 private StringBuilder collectBeatMessage(String strGUID) {
		// TODO Auto-generated method stub
		StringBuilder sbXML = null;
		boolean bQueued = false;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			bQueued = collectorManager.collectBeatData(strGUID);
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

	public static String makeValidVarchar(String str) {
		  StringBuilder sbValue = new StringBuilder();
		  
		  if( str == null )
		   sbValue.append("null");
		  else
		   sbValue.append("'").append(str.replaceAll("'","''")).append("'");
		  
		  return sbValue.toString();
		 }
	/**
	 * sending breach counters to SLA collector to insert into 
	 * table 
	 * @param strAgentType
	 * @param strGUID
	 * @param strSlaBreachCounterset
	 * @return
	 */
	public StringBuilder sendToSlaCollector(String strAgentType, String strSlaBreachCounterset, String strGUID) {
		StringBuilder sbResponse= null;
		HttpClient client = new HttpClient();
		PostMethod method = null;
		String responseJSONStream = null;
		
		method = new PostMethod(Constants.APPEDO_SLA_COLLECTOR_URL);
		
		try{
			LogManager.infoLog(strAgentType.toString()+" sending breach counters: "+strSlaBreachCounterset);
			
			method.setParameter("guid", strGUID);
			method.setParameter("slaBreachCounterset", strSlaBreachCounterset);
			method.setParameter("command", "breach_counter_set");
			int statusCode = client.executeMethod(method);
			LogManager.infoLog(Constants.APPEDO_SLA_COLLECTOR_URL+" - statusCode : "+statusCode);
			
			if (statusCode != HttpStatus.SC_OK) {
				LogManager.errorLog("URL failed: " + method.getStatusLine());
			}
			try {
				responseJSONStream = method.getResponseBodyAsString();
				sbResponse = UtilsFactory.getJSONSuccessReturn("Data sent.");
			} catch (HttpException he) {
				LogManager.errorLog(he);
				sbResponse = UtilsFactory.getJSONFailureReturn("Unable to sent data to sla collector");
			}
		} catch (IOException ie) {
			LogManager.errorLog(ie);
			sbResponse = UtilsFactory.getJSONFailureReturn("Unable to sent data to sla collector");
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.errorLog(e);
			sbResponse = UtilsFactory.getJSONFailureReturn("Unable to sent data to sla collector");
		} finally {
			method.releaseConnection();
			method = null;				
			responseJSONStream = null;
		}
		
		return sbResponse;
	}
	
	
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private StringBuilder collectMessage(String strAgentType, String strCounterParams,String strGUID) {
		StringBuilder sbXML = null;
		boolean bQueued = false;
		
		CollectorManager collectorManager = null;
		Connection con = null;
		
		try {
			if (ModulePerformanceCounterManager.hmAgentStatus.containsKey(strGUID)) {
				sbXML = UtilsFactory.getJSONSuccessReturn("kill");
			} else {
				collectorManager = CollectorManager.getCollectorManager();
				
				bQueued = collectorManager.collectPerformanceCounters(strCounterParams);
//				System.out.println(UtilsFactory.nowFormattedDate(true)+" - "+strAgentType+"-PerfCounter "
//									+nThreadId+"%="+nIndex+" queue.size: "+collectorManager.getCounterLength(nIndex));
//				System.out.println("bQueued status  " + bQueued);
				if( bQueued ) {
					
					if(strGUID != null ) {
						
						HashMap<String, Object> hmKeyValues = null;
						hmKeyValues = new HashMap<String, Object>();
//						System.out.println("Inside  bQueued " + strGUID);
						// Monitor counters update
						JSONArray jaCounters = CollectorManager.getCounters(strGUID);						
						CollectorManager.removeCounterSet(strGUID);
						// Sla counters updates
						JSONArray jaSlaCounters = CollectorManager.getSlaCounters(strGUID);						
						CollectorManager.removeSlaCounterSet(strGUID);
						
						
						if(jaSlaCounters!=null) {
							hmKeyValues.put("SlaCounterSet", jaSlaCounters);
							//sbXML = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
						}
						CollectorManager.removeCounterSet(strGUID);
						
						// check agent upgraded version is available 
//						if(CollectorManager.hmUpgradeAgentBean.containsKey(strGUID)) {
//							
//							hmKeyValues.put("moduleupgrade", true);
//							hmKeyValues.put("upgrade_details", CollectorManager.hmUpgradeAgentBean.get(strGUID));
//							CollectorManager.hmUpgradeAgentBean.remove(strGUID);
//						}
						
						if(jaCounters!=null) {
							hmKeyValues.put("MonitorCounterSet", jaCounters);
							
							con = DataBaseManager.giveConnection();
							
							// update counter master table column & last sent time column						
							collectorManager.updateStatusOfCounterMaster(con, strGUID);
						}
						
						// response to the received data from client 
						hmKeyValues.put("message", "Data queued.");
						sbXML = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
					}
					
				} else {
					sbXML = UtilsFactory.getJSONFailureReturn("Unable to queue data. Try again later.");
				}
			}
		} catch (Exception e) {
			LogManager.errorLog(e);			
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
//		System.out.println("sbXML = " +sbXML.toString());
		return sbXML;
	}
	
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private StringBuilder collectProfilerArrayForProcedure(AGENT_TYPE agent_type, String strGUID, String strTimerThreadId, String strProfilerArray) {
		StringBuilder sbXML = null;
		boolean bQueued = false;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			
			
			if(agent_type == AGENT_TYPE.MSSQL ){
				bQueued = collectorManager.collectMSSQLProcedures(strGUID, strProfilerArray);
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
	private StringBuilder collectProfilerArray(AGENT_TYPE agent_type, String strGUID, String strTimerThreadId, String strProfilerArray) {
		StringBuilder sbXML = null;
		boolean bQueued = false;
		CollectorManager collectorManager = null;
		
		try {
			collectorManager = CollectorManager.getCollectorManager();
			
			if( agent_type == AGENT_TYPE.JAVA_PROFILER ) {
				bQueued = collectorManager.collectJavaProfiler(strGUID, "{1001: \""+strGUID+"\", profilerArray: "+strProfilerArray+", timerThreadId: "+strTimerThreadId+"}");
			}
			else if( agent_type == AGENT_TYPE.DOTNET_PROFILER ) {
				bQueued = collectorManager.collectDotNetProfiler(strGUID, "{1001: \""+strGUID+"\", profilerArray: "+strProfilerArray+"}");
			}
			else if( agent_type == AGENT_TYPE.POSTGRES ) {
				bQueued = collectorManager.collectPGSlowQueries(strGUID, strProfilerArray);
			}
			else if( agent_type == AGENT_TYPE.MSSQL ) {
				bQueued = collectorManager.collectMSSQLSlowQueries(strGUID, strProfilerArray);
			}
			else if( agent_type == AGENT_TYPE.MYSQL ) {
				bQueued = collectorManager.collectMySQLSlowQueries(strGUID, strProfilerArray);
			}
			else if( agent_type == AGENT_TYPE.ORACLE ) {
				bQueued = collectorManager.collectOracleSlowQueries(strGUID, strProfilerArray);
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
			PostMethod method = new PostMethod("http://localhost:8080/SS_Profiler/collectCounters");
			
			method.setParameter("agent_type", "MYSQL");
			method.setParameter("counter_params_json", "{uid: 100011, client_id: 1, database_server_id: 12, aborted_clients: 10, aborted_connects: 20, bytes_received: 46, bytes_sent: 4, database_version: 4.0, disk_key_reads: 0, disk_key_writes: 30, hander_read_random: 2, hander_read_random_next: 2, handler_rollback: 1, handler_savepoint: 5, handler_savepoint_rollback: 0, key_buffer_size_percentage: 80, max_used_connections: 60, not_flushed_delayed_rows: 10, select_full_join: 12, select_range_check: 0, slow_lanch_threads: 3, slow_queries: 7, sort_merge_passes: 0, table_locks_waited: 0, threads_connected: 3, threads_running: 2}");
			
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
		}
	}

}
