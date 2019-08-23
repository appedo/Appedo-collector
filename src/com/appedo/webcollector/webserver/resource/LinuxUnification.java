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
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONObject;

public class LinuxUnification extends Resource{

	public LinuxUnification(Context context, Request request, Response response) {
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
		Connection con = null;
		
		try {
			Form frm = new Form(entity);	

			String strCommand = UtilsFactory.replaceNull(frm.getFirstValue("command"), "");
			
			String strSysInformation = frm.getFirstValue("systemInformation");
			
			JSONObject sysAllInformation = JSONObject.fromObject(strSysInformation);
			
			//JSONObject testSysInfo = JSONObject.fromObject(frm.getFirstValue("systemInformation"));
			
			switch(strCommand) {
				
				case "systemGeneratorInfo" : {
					
					con = DataBaseManager.giveConnection();
					
					String sysGeneratorUUID = frm.getFirstValue("systemGeneratorUUID");
					
					responseXml = getSystemId(con, sysGeneratorUUID);
					
					break;
				}
				
				case "systemInformation" : {
					
					con = DataBaseManager.giveConnection();
					
					JSONObject systemInformation = (JSONObject) sysAllInformation.get("systemInformation");
					
					String Encrypted_id = frm.getFirstValue("Encrypted_id");
					
					String VMVARE_Key = frm.getFirstValue("VMWARE_Key");
					
					responseXml = collectSystemIdMessage(con, Encrypted_id, systemInformation, VMVARE_Key);
					
					break;
				}
				
				case "serverInformation" : {
					
					con = DataBaseManager.giveConnection();
					
					JSONObject serverInformation = JSONObject.fromObject(frm.getFirstValue("moduleInformation"));
					
					JSONObject joCounterSet = JSONObject.fromObject(frm.getFirstValue("jocounterSet"));
					
					long systemId = Long.parseLong(frm.getFirstValue("systemId"));
					
					String UUID = frm.getFirstValue("UUID");
					
					String Enterprise_Id = frm.getFirstValue("Enterprise_Id");
					
					responseXml = createServerModule(con, serverInformation, systemId, UUID, Enterprise_Id, joCounterSet);
					
					break;
				}
				
				case "appInformation" : {
					
					con = DataBaseManager.giveConnection();
					
					JSONObject appInformation = JSONObject.fromObject(frm.getFirstValue("moduleInformation"));
					
					JSONObject joCounterSet = JSONObject.fromObject(frm.getFirstValue("jocounterSet"));
					
					long systemId = Long.parseLong(frm.getFirstValue("systemId"));
					
					String UUID = frm.getFirstValue("UUID");
					
					String Enterprise_Id = frm.getFirstValue("Enterprise_Id");
					
					responseXml = createApplicationModule(con, appInformation, systemId, UUID, Enterprise_Id, joCounterSet);
					
					break;
				}
				
				case "DBInformation" : {
					
					con = DataBaseManager.giveConnection();
					
					JSONObject appInformation = JSONObject.fromObject(frm.getFirstValue("moduleInformation"));
					
					JSONObject joCounterSet = JSONObject.fromObject(frm.getFirstValue("jocounterSet"));
					
					long systemId = Long.parseLong(frm.getFirstValue("systemId"));
					
					String UUID = frm.getFirstValue("UUID");
					
					String Enterprise_Id = frm.getFirstValue("Enterprise_Id");
					
					responseXml = createDataBaseModule(con, appInformation, systemId, UUID, Enterprise_Id, joCounterSet);
					
					break;
				}
				
				case "moduleRunningStatus" : {
					
					con = DataBaseManager.giveConnection();
					
					String GUID = frm.getFirstValue("GUID");
					
					responseXml = moduleRunningStatus(con, GUID);
					
					break;
				}
				
				case "UpdateJbossAppInfo" : {
					
					con = DataBaseManager.giveConnection();
					
					String GUID = frm.getFirstValue("GUID");
					
					JSONObject joJbossInfo = JSONObject.fromObject(frm.getFirstValue("moduleInformation"));
					
					responseXml = updateJbossInfo(con, GUID, joJbossInfo);
					
					break;
				}

				default  : {
					//responseXml = collectMessage(strAgentType, strCounterParams, strGUID);
					break;
				}
			}
			
		}catch (Exception e) {
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

	private StringBuilder getSystemId(Connection con, String sysGeneratorUUID) {
		// TODO Auto-generated method stub
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		JSONObject ResponseMessage = new JSONObject();
		try {
			collectorManager = CollectorManager.getCollectorManager();
			ResponseMessage = collectorManager.getSystemId(con, sysGeneratorUUID);
			sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage.getString("message"), "systemId", ResponseMessage.getString("systemId"));
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		return sbXML;
	}
	
	
	private StringBuilder collectSystemIdMessage(Connection con, String Encrypted_id, JSONObject sysInformation, String VMVARE_Key) {
		// TODO Auto-generated method stub
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		JSONObject ResponseMessage = new JSONObject();
		try {
			collectorManager = CollectorManager.getCollectorManager();
			ResponseMessage = collectorManager.collectServerData(con, sysInformation, Encrypted_id, VMVARE_Key);
			sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage.getString("message"), "systemId", ResponseMessage.getString("systemId"));
		} catch (Exception e) {
			LogManager.errorLog(e);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		return sbXML;
	}
	
	private StringBuilder createServerModule(Connection con, JSONObject serverInformation, long systemId, String UUID, String EnterpriseId, JSONObject joCounterSet) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		JSONObject ResponseMessage = new JSONObject();
		
		try {
			
			collectorManager = CollectorManager.getCollectorManager();
			
			ResponseMessage = collectorManager.createServerModule(con, serverInformation, systemId, UUID, EnterpriseId, joCounterSet);
			
			//sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage.getString("message"), "moduleGUID", ResponseMessage.getString("moduleGUID"));
			
			sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage);
			
		}catch(Exception ex) {
			LogManager.errorLog(ex);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( ex.getMessage() );
		}
		return sbXML;
	}
	
	private StringBuilder createApplicationModule(Connection con, JSONObject moduleInformation, long systemId, String UUID, String EnterpriseId, JSONObject joCounterSet) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		JSONObject ResponseMessage = new JSONObject();
		
		try {
			
			collectorManager = CollectorManager.getCollectorManager();
			
			ResponseMessage = collectorManager.createApplicationModule(con, moduleInformation, systemId, UUID, EnterpriseId, joCounterSet);
			
			//sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage.getString("message"), "moduleGUID", ResponseMessage.getString("moduleGUID"));
			
			sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage);
			
		}catch(Exception ex) {
			LogManager.errorLog(ex);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( ex.getMessage() );
		}
		return sbXML;
	}
	
	private StringBuilder createDataBaseModule(Connection con, JSONObject moduleInformation, long systemId, String UUID, String EnterpriseId, JSONObject joCounterSet) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		JSONObject ResponseMessage = new JSONObject();
		
		try {
			
			collectorManager = CollectorManager.getCollectorManager();
			
			ResponseMessage = collectorManager.createDataBaseModule(con, moduleInformation, systemId, UUID, EnterpriseId, joCounterSet);
			
			sbXML = UtilsFactory.getJSONSuccessReturn(ResponseMessage);
			
		}catch(Exception ex) {
			LogManager.errorLog(ex);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( ex.getMessage() );
		}
		return sbXML;
	}
	
	private StringBuilder moduleRunningStatus(Connection con, String GUID) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		String strModuleStatus;
		try {
			
			collectorManager = CollectorManager.getCollectorManager();
			
			strModuleStatus = collectorManager.moduleRunningStatus(con, GUID);
			
			sbXML = UtilsFactory.getJSONSuccessReturn(strModuleStatus);
			
		}catch(Exception ex) {
			LogManager.errorLog(ex);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( ex.getMessage() );
		}
		return sbXML;
	}
	
	private StringBuilder updateJbossInfo(Connection con, String GUID, JSONObject joJbossInfo) {
		StringBuilder sbXML = null;
		CollectorManager collectorManager = null;
		try {
			
			collectorManager = CollectorManager.getCollectorManager();
			
			collectorManager.updateJbossInfo(con, GUID, joJbossInfo);
			
			sbXML = UtilsFactory.getJSONSuccessReturn("successfully updated jboss information");
			
		}catch(Exception ex) {
			LogManager.errorLog(ex);
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( ex.getMessage() );
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
		/*HttpClient client = null;
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
		}*/
		
		String name = "Ubuntu 16.04.3 LTS";
		
		String[] name1 = name.split(" ");
		
		System.out.println("Result : "+ name1[0]);
	}
	
}
