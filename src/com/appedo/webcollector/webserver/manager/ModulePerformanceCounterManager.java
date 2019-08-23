package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.CollectorParentDBI;
import com.appedo.webcollector.webserver.bean.CollectorBean;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * This is Module counter related manager class.
 * This will queue the counter-set into batch and insert them into database.
 * 
 * @author Ramkumar R
 *
 */
public class ModulePerformanceCounterManager {
	
	// Database Connection object. Single connection will be maintained for entire operations.
	protected Connection conPC = null;
	
	protected CollectorParentDBI collectorDBI = null;
	
	protected CollectorManager collectorManager = null;
	
	public static HashMap<String,Object> hmAgentStatus = new HashMap<String,Object>();
	
	
	// TODO enable_1min 
	// private HashMap<Integer, Double> hmCounterForMin = new HashMap<Integer, Double>();
	
	/**
	 * Avoid multiple object creation, by Singleton
	 * Do the initialization operations like
	 * Connection object creation and CollectorManager's object creation.
	 * 
	 * @throws Exception
	 */
	public ModulePerformanceCounterManager() throws Exception {
		collectorDBI = new CollectorParentDBI();
		
		establishDBonnection();
		
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	protected void establishDBonnection(){
		try{
			conPC = DataBaseManager.giveConnection();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
	}
	
	public void migrationCounterModules(CollectorBean cbCounterEntry, long lThisThreadId) throws Exception {
		System.out.println("Migration counter start!");
		PostMethod method = null;
		HttpClient client = null;
		CollectorDBI collectorDBI = null;
		String responseJSONStream = null;
		
		try {
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			collectorDBI = new CollectorDBI();
			
			//long lSystemId = collectorDBI.getSystemIdV2(conPC, cbCounterEntry.getStrUUID());
			long lSystemId = collectorDBI.getSystemId(conPC, cbCounterEntry.getStrUUID());
			
			//update Status json in server information Table
			collectorDBI.updateAgentProgressStatus(conPC, cbCounterEntry.getStrUUID(), cbCounterEntry.getStrModuleName());
			
			method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/migrationModuleCounters");
			method.setParameter("UUID", cbCounterEntry.getStrUUID());
			method.setParameter("systemId", String.valueOf(lSystemId));
			method.setParameter("oldGUID", cbCounterEntry.getStrOldGUID());
			method.setParameter("moduleTypeName", cbCounterEntry.getStrModuleName());
			method.setParameter("moduleVersion", cbCounterEntry.getStrModuleVersion());
			method.setParameter("JocounterSet", cbCounterEntry.getJSONCounterParams().toString());
			

			client = new HttpClient();
			int statusCode = client.executeMethod(method);
			
			method.setRequestHeader("Connection", "close");
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			} else {
				responseJSONStream = method.getResponseBodyAsString();

				if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
					
					JSONObject joResp = JSONObject.fromObject(responseJSONStream);
					
					if( joResp.getBoolean("success") ){
						collectorDBI.DeleteAgentProgressJson(conPC, cbCounterEntry.getStrUUID(), cbCounterEntry.getStrModuleName());
					}else{
						//update Message column set errorMessage
						collectorDBI.updateAgentMessage(conPC, cbCounterEntry.getStrUUID(), cbCounterEntry.getStrModuleName(), joResp.getString("errorMessage"));
					}
				}
			}
		}catch(Exception e) {
			System.out.println(e);
			LogManager.errorLog(e);
		}
		
	}
	
	
	public JSONObject migrationCounterModulesV2(JSONObject joModuleData, String strModuleName, String strUUID) throws Exception {
		
		PostMethod method = null;
		HttpClient client = null;
		CollectorDBI collectorDBI = null;
		String responseJSONStream = null;
		JSONObject joResponse = null;
		
		try {
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			collectorDBI = new CollectorDBI();
			joResponse = new JSONObject();
			
			//long lSystemId = collectorDBI.getSystemIdV2(conPC, cbCounterEntry.getStrUUID());
			long lSystemId = collectorDBI.getSystemId(conPC, strUUID);
			
			//update Status json in server information Table
			//collectorDBI.updateAgentProgressStatus(conPC, strUUID, strModuleName);
			
			method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/migrationModuleCounters");
			method.setParameter("UUID", strUUID);
			method.setParameter("systemId", String.valueOf(lSystemId));
			method.setParameter("oldGUID", joModuleData.getString("oldGuid"));
			method.setParameter("moduleTypeName", strModuleName);
			method.setParameter("moduleVersion", joModuleData.getString("version"));
			method.setParameter("JocounterSet", joModuleData.getJSONObject("counterSet").toString());
			

			client = new HttpClient();
			int statusCode = client.executeMethod(method);
			
			
			method.setRequestHeader("Connection", "close");
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			} else {
				responseJSONStream = method.getResponseBodyAsString();

				if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
					
					JSONObject joResp = JSONObject.fromObject(responseJSONStream);
					if( joResp.getBoolean("success") ){
						joResponse.put("joResponse", joResp.getString("message"));
						//collectorDBI.DeleteAgentProgressJson(conPC, strUUID, strModuleName);
					}else{
						joResponse.put(strModuleName, "unable to migrate in Windows Agent");
						//update Message column set errorMessage
						//collectorDBI.updateAgentMessage(conPC, strUUID, strModuleName, joResp.getString("errorMessage"));
					}
				}
			}
		}catch(Exception e) {
			System.out.println(e);
			LogManager.errorLog(e);
		}
		return joResponse;
	}
		
	public boolean isGuidExist(String strOldGuid) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		boolean bGuidExists = false;
		String strQuery = "";
		try {
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			strQuery = "SELECT EXISTS( SELECT guid from module_master WHERE guid = ?) AS guid_exists";
			pstmt = conPC.prepareStatement(strQuery);
			pstmt.setString(1, strOldGuid);
			rst = pstmt.executeQuery();
			if(rst.next()) {
				bGuidExists = rst.getBoolean("guid_exists");
			}
			strQuery = null;
		} catch (Exception e) {
			LogManager.errorLog(e);
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
		}
		return bGuidExists;
	}
	
	/***
	 * 
	 * @param cbCounterEntry
	 * @param lThisThreadId
	 * @throws Exception
	 */
	public JSONObject insertAndCreateModulesV2(JSONObject joModuleData, String strModuleName, String strUUID) throws Exception {
		
		PostMethod method = null;
		HttpClient client = null;
		
		String responseJSONStream = null;
		
		JSONObject joSystemData = null, joResponse = null;
		String strModuleType;
		CollectorDBI collectorDBI = null;
		boolean redirect = true;
		int limit = 0;
		try {
			collectorDBI = new CollectorDBI();
			joSystemData = new JSONObject();
			joResponse = new JSONObject();
			
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			joSystemData = collectorDBI.getSystemData(conPC, strUUID, strModuleName, "WINDOWS");

			//collectorDBI.updateAgentProgressStatus(conPC, strUUID, strModuleName);
			
			strModuleType = strModuleName.equals("WINDOWS") ? "SERVER" : (strModuleName.equals("MSSQL") ? "DATABASE" : "APPLICATION");
						
			method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/addModuleFromCollector");
			method.setParameter("moduleName", joSystemData.getString("moduleName"));
			method.setParameter("moduleTypeName", strModuleName);
			method.setParameter("moduleDescription", joSystemData.getString("moduleName"));
			method.setParameter("moduleVersion", joModuleData.getString("version"));
			method.setParameter("moduleType", strModuleType);
			//method.setParameter("clrVersion", "");
			method.setParameter("JocounterSet", joModuleData.getJSONObject("counterSet").toString());
			method.setParameter("UUID", strUUID);
			method.setParameter("systemId", joSystemData.getString("systemId"));
			method.setParameter("userId", joSystemData.getString("userId"));
			method.setParameter("e_id", joModuleData.getString("eid"));
			
			while(redirect){
				client = new HttpClient();
				int statusCode = client.executeMethod(method);
				
				System.out.println("statusCode: "+ statusCode);
				
				method.setRequestHeader("Connection", "close");
				if (statusCode != HttpStatus.SC_OK) {
					System.err.println("Method failed: " + method.getStatusLine());
				} else {
					responseJSONStream = method.getResponseBodyAsString();
					
					
					if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
						
						JSONObject joResp = JSONObject.fromObject(responseJSONStream);
						
						if( joResp.getBoolean("success") ){
							
							joResponse.put("joResponse", joResp.getString("message"));
							//collectorDBI.DeleteAgentProgressJson(conPC, strUUID, strModuleName);
							redirect = false;
						}else{
							//update Message column set errorMessage
							if(joResp.getString("errorMessage").startsWith("License")){
								//collectorDBI.updateAgentMessage(conPC, strUUID, strModuleName, joResp.getString("errorMessage"));
								redirect = false;
							}else{
								Thread.sleep( Constants.MODULE_INSERT_REST_MILLESECONDS);
								if(limit>3){ redirect = false;}
								limit++;
							}
						}
					}
				}
			}
					
		} catch(Exception e) {
			System.out.println(e);
			LogManager.errorLog(e);
		}
		return joResponse;
	}
	
	/**
	 * Retrieves the counter-sets from the respective queue.
	 * And inserts them in the database.
	 * 
	 * @throws Exception
	 */
	public void fetchCounter(CollectorBean cbCounterEntry, long lThisThreadId) throws Exception {
		int nBatchSize = 0;
		
		JSONArray jaCounterEntry = null;
		JSONObject joCounterEntry = null;
		
		String strCounterParams = "";
		
		long lUID = -1L;
		
		Set<Long> hsCollectedUIds = null;
		
		try{
			// if connection is not established to db server then wait for 10 seconds
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			strCounterParams = cbCounterEntry.getCounterParams();
			
			/*
			 * Older agents, which was downloaded bfr Warning-Critical threshold implementation,
			 * will return `JSONObject["thresholdvalue"] is not a number.` in error key 1005.
			 * So removed double-quote to do JSON parsing.
			 */
			strCounterParams = strCounterParams.replaceAll("\"thresholdvalue\"", "thresholdvalue");
			
			// Agents developed are sending bath of data for each minute. So they come as Array
			if( strCounterParams.startsWith("[") ) {
				jaCounterEntry = JSONArray.fromObject( strCounterParams );
				nBatchSize = jaCounterEntry.size();
			}
			// Agents developed by Rasith is sending data for each 20 sec. They come as Object
			else if( strCounterParams.startsWith("{") ) {
				jaCounterEntry = new JSONArray();
				jaCounterEntry.add( JSONObject.fromObject( strCounterParams ) );
				nBatchSize = 1;
			}
			else {
				throw new Exception("Counter-set[s] sent is not in JSON format");
			}
			hsCollectedUIds = new HashSet<Long>();
			
			collectorDBI.initializeInsertBatch(conPC);
			
			for( int nBatchIndex=0; nBatchIndex<jaCounterEntry.size(); nBatchIndex++ ){
				
				joCounterEntry = jaCounterEntry.getJSONObject(nBatchIndex);
				
				// subtract 20 second interval respectively for each counter-set in this batch
				// if batch length is 3 then,
				// 3rd counter-set - cbCounterEntry.getReceivedOn()
				// 2nd counter-set - cbCounterEntry.getReceivedOn() - 20 second
				// 1st counter-set - cbCounterEntry.getReceivedOn() - 40 second
				joCounterEntry.put("1003", cbCounterEntry.getReceivedOn().getTime() - (1000*20)*(nBatchSize-(nBatchIndex+1)));
				
				// SLA for LINUX
				// add the keys for SLA
				/* TODO enable_SLA
				if( this.agent_family.equals(AGENT_FAMILY.SERVER) ){
					Iterator<String> iter = joCounterEntry.keys();
					while( iter.hasNext() ){
						String strKey = iter.next();
						
						try{
							// avoid Appedo counters like UID(1001), client-timestamp(1002) & others.
							if( ! (strKey.startsWith("100") && strKey.length() == 4) ){
								collectorManager.addLatestCounter( strKey+"_"+joCounterEntry.getString("1001"), new Object[]{new Date(), joCounterEntry.getDouble(strKey)} );
							}
						} catch(Exception e) {
							System.out.println("Exception in fetchCounter.double: "+e.getMessage());
						}
					}
				}
				*/
				// SLA end
				
				lUID = addCounterBatch( joCounterEntry );
				
				// In future, agent can send multiple UIds data. Ex.: one agent can send Tomcat, MySQL, Linux GUIDs data.
				// So keeping unique refer in an object.
				hsCollectedUIds.add(lUID);
				
				if (nBatchIndex != jaCounterEntry.size() - 1) {
					UtilsFactory.clearCollectionHieracy(joCounterEntry);
					joCounterEntry = null;
				}
			}
			
			// insert the counter as batch
			long dbStartTime = System.currentTimeMillis();
			executeCounterBatch(lThisThreadId);
			long dbTotalTime = System.currentTimeMillis() - dbStartTime;
			if (dbTotalTime > 1000) {
				LogManager.infoLog("OtherCollectorDBInsTimeForSize:"+jaCounterEntry.size()+"Statement:"+dbTotalTime);
				Thread.sleep(500);
			}
			
			// updates uid's last appedo received on
			for (long lCollectedUId : hsCollectedUIds) {
				collectorDBI.updateModuleLastAppedoReceivedOn(conPC, lCollectedUId, cbCounterEntry.getReceivedOn().getTime());					
			}
			
			
			// Queue Counter Averages along with last counter-set's summary detail(1001,1002...)
			// TODO change the prestmt in CounterDBI.java and enable this
			// TODO enable_1min addCounterBatchFor1Min(hmCounterForMin, joCounterEntry);
			
			UtilsFactory.clearCollectionHieracy(joCounterEntry);
			joCounterEntry = null;
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			UtilsFactory.clearCollectionHieracy(joCounterEntry);
			joCounterEntry = null;
			
			UtilsFactory.clearCollectionHieracy(jaCounterEntry);
			jaCounterEntry = null;
			
			UtilsFactory.clearCollectionHieracy(cbCounterEntry);
			cbCounterEntry = null;
			
			DataBaseManager.close(conPC);
			conPC = null;
		}
	}
	
	/**
	 * Add the counter-set received to the queue, along with current time to keep track of received time.
	 * 
	 * @param joCounterEntry
	 * @throws Exception
	 */
	protected long addCounterBatch(JSONObject joCounterEntry) {
		Iterator<String> iterCounterNames = null;

		String strCounterCode = null, strAgentTimeStamp = null, agent_version = null, strAgentError = null, strCounterError = null;
		Integer nCounterCode = 0;
		Double dCounterValue = 0.0;
		JSONObject joErrors = null;
		JSONArray joStaticCounterIds = null;
		String strTopProcValue = "{}";
		boolean is_first = false;

		long lUID = 0l, lAppedoTimeStamp = 0l;
		Timestamp tsAgent = null, tsAppedo = null;

		try {
			lUID = getUID(joCounterEntry.getString("1001"));
			if (lUID == -1l) {
				hmAgentStatus.put(joCounterEntry.getString("1001"), new Date());
				throw new Exception("Given GUID is not matching: " + joCounterEntry.getString("1001"));
			}
			
			strAgentTimeStamp = joCounterEntry.getString("1002");
			tsAgent = new Timestamp(UtilsFactory.toDate(strAgentTimeStamp, "yyyy-MM-dd HH:mm:ss.S").getTime());
			lAppedoTimeStamp = joCounterEntry.getLong("1003");
			tsAppedo = new Timestamp(lAppedoTimeStamp);
			
			if (joCounterEntry.containsKey("1004")) {
				agent_version = joCounterEntry.getString("1004");
			}
			
			if (joCounterEntry.containsKey("1005")) {
				joErrors = joCounterEntry.getJSONObject("1005");
				if (joErrors.containsKey("1006")) {
					strAgentError = joErrors.getString("1006");
				}
			}
			
			if (joCounterEntry.containsKey("1007")) {
				joStaticCounterIds = joCounterEntry.getJSONArray("1007");
			}
			
			// lAgentResponseId = collectorDBI.insertAgentResponse(lUID, tsAgent, tsAppedo, agent_version, strAgentError);
			iterCounterNames = (Iterator<String>) joCounterEntry.keys();
			while (iterCounterNames.hasNext()) {
				strCounterCode = "";
				strTopProcValue = "{}";
				is_first = false;

				strCounterCode = iterCounterNames.next();

				// avoid Appedo functionality counters, which starts from 1000 to 1999
				if (!(strCounterCode.length() == 4 && strCounterCode.startsWith("1")) && !strCounterCode.endsWith("_TOP")) {
					// Get exception of this counter(strCounterCode)
					if (joErrors != null && joErrors.containsKey(strCounterCode)) {
						strCounterError = joErrors.getString(strCounterCode);
					}
					// To insert 'true' for the is_first column , check whether the map has counter_id's
					// of static counters
					if (joStaticCounterIds != null) {
						is_first = joStaticCounterIds.contains(strCounterCode);
					}

					try {

						// if(strCounterCode.trim().endsWith("_TOP")) {
						//
						// strTopProcValue = joCounterEntry.get(strCounterCode).toString();
						// strCounterCode = strCounterCode.replaceAll("_TOP", "");
						// }else {

						if (joCounterEntry.containsKey(strCounterCode + "_TOP")) {
							strTopProcValue = joCounterEntry.get(strCounterCode + "_TOP").toString();
						}

						dCounterValue = new Double((long) joCounterEntry.get(strCounterCode)).doubleValue();
						// }

					} catch (ClassCastException ex1) {
						try {
							dCounterValue = new Double((Integer) joCounterEntry.get(strCounterCode)).doubleValue();
						} catch (ClassCastException ex2) {
							dCounterValue = (Double) joCounterEntry.get(strCounterCode);
						}
					}

					nCounterCode = Integer.parseInt(strCounterCode);

					/* TODO enable_1min 
					// Add Counter Values for 1 minute average
					if (hmCounterForMin.containsKey(nCounterCode)) {
						hmCounterForMin.put(nCounterCode, hmCounterForMin.get(nCounterCode) + dCounterValue);
					} else {
						hmCounterForMin.put(nCounterCode, dCounterValue);
					}
					*/
					if (!strCounterCode.endsWith("_TOP")) {
						collectorDBI.addCounterBatch(lUID, tsAgent, tsAppedo, agent_version, nCounterCode, dCounterValue, strCounterError, strTopProcValue, is_first);
					}

				}
			}
		} catch (Throwable th) {
			LogManager.errorLog(th);
		} finally {
			iterCounterNames = null;
			strCounterCode = null;
			agent_version = null;
			strAgentError = null;
			strCounterError = null;
			joErrors = null;
		}

		return lUID;
	}
	
	/* TODO enable_1min 
	protected void addCounterBatchFor1Min(HashMap<Integer, Double> counterInMin, JSONObject joCounterEntry) throws Exception {
		long lAgentResponseId = 0;
		long lUID = getUID(joCounterEntry.getString("1001"));
		String strAgentTimeStamp = joCounterEntry.getString("1002");
		Timestamp tsAgent = new Timestamp(UtilsFactory.toDate(strAgentTimeStamp, "yyyy-MM-dd HH:mm:ss.S").getTime());
		long lAppedoTimeStamp = joCounterEntry.getLong("1003");
		Timestamp tsAppedo = new Timestamp(lAppedoTimeStamp);
		String agent_version = "";
		if( joCounterEntry.containsKey("1004") ){
			agent_version = joCounterEntry.getString("1004");
		}
		
		String strAgentError = null, strCounterError = null;
		JSONObject joErrors = null;
		if (joCounterEntry.containsKey("1005")) {
			joErrors = joCounterEntry.getJSONObject("1005");
			if (joErrors.containsKey("1006")) {
				strAgentError = joErrors.getString("1006");
			}
		}
		
		/*
		lAgentResponseId = collectorDBI.insertAgentResponseInMin(lUID, tsAgent, tsAppedo, agent_version, strAgentError);
		for (Map.Entry<Integer, Double> e : hmCounterForMin.entrySet()) {
			if (joErrors != null && joErrors.containsKey(e.getKey())) {
				strCounterError = joErrors.getString(String.valueOf(e.getKey()));
			}
			// TODO enable_1min collectorDBI.addCounterBatchInMin(lAgentResponseId, e.getKey(), Math.floor(e.getValue() / 3), strCounterError);
		}
		*
	}*/
	
	/**
	 * Returns the UID of the Application for the given encrypted UID, for this agent.
	 * 
	 * @param strEncryptedUID
	 * @return
	 * @throws Exception
	 */
	private Long getUID(String strEncryptedUID) throws Exception {
		Long lUID = null;
		CollectorDBI collectorDBI = null;
		
		try{
			collectorDBI = new CollectorDBI();
			
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			lUID = collectorDBI.getModuleUID(conPC, strEncryptedUID);
		} catch ( Throwable th ) {
			throw th;
		} finally {
			collectorDBI = null;
		}
		
		return lUID;
	}
	
	/**
	 * Execute the batched counter-set inserts
	 * 
	 * @throws Exception
	 */
	protected void executeCounterBatch(long lThisThreadId) throws Throwable {
		collectorDBI.executeCounterBatch(lThisThreadId);
	}
	
	
	@Override
	protected void finalize() throws Throwable {
		
		// close the connection before this object is destroyed from JVM
		DataBaseManager.close(conPC);
		conPC = null;
		
		super.finalize();
	}
}
