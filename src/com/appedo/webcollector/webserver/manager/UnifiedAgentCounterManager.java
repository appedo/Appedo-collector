package com.appedo.webcollector.webserver.manager;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.CollectorParentDBI;
import com.appedo.webcollector.webserver.bean.UnifiedCounterDataBean;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class UnifiedAgentCounterManager {
	
	protected Connection conPC = null;
	
	protected CollectorParentDBI collectorDBI = null;
	
	protected CollectorManager collectorManager = null;

	public UnifiedAgentCounterManager() throws Exception {
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
	
	public void fetchCounters(JSONArray jaCounterDatas) throws Exception {
		
		UnifiedCounterDataBean uaCounterDataBean = null;
		CallableStatement callableStmt = null;
		PreparedStatement pstmt = null;
		int remainingData = 0;
		long lUID = -1L, lUserId = -1L;
		int insertedCount = 0;
		JSONObject joModuleDetails = null;
		HashMap<Long, String> hmCollectedUIds = null;
		Set<Long> hsUserIds = null;
		
		try{
			uaCounterDataBean = new UnifiedCounterDataBean(); 
			
			while (true) {
				conPC = DataBaseManager.reEstablishConnection(conPC);
			
				hmCollectedUIds = new HashMap<Long, String>();
				hsUserIds = new HashSet<Long>();
				
				callableStmt = collectorDBI.createCollectorTablePrepredstmt(conPC);
				remainingData = jaCounterDatas.size(); 
				for (int count = 0; count < jaCounterDatas.size(); count++) {
					uaCounterDataBean.setCounterData(jaCounterDatas.getJSONObject(count).getJSONObject("counterData"));
					
					joModuleDetails = getModuleDetail(uaCounterDataBean.getCounterData().getString("guid"));
					
					//lUID = getUID(uaCounterDataBean.getCounterData().getString("guid"));
					
					
					if (joModuleDetails.size() > 0) {
						
						lUID = joModuleDetails.getLong("uid");
						lUserId = joModuleDetails.getLong("userId");
						
						String incomingDate = uaCounterDataBean.getCounterData().getString("datetime");
						
						// if SLA breached data received, get unique user Id to update in user_pvt_counters
						if (uaCounterDataBean.getCounterData().containsKey("SLASet")) {
							hsUserIds.add(lUserId);
						}
						// if counters data received, get UID & latest to update last_appedo_received_on in module_master
						else {
							if (hmCollectedUIds.containsKey(lUID)) {
								String existingDate = hmCollectedUIds.get(lUID);
								if(UtilsFactory.formatISOTimeZoneToLong(existingDate) < UtilsFactory.formatISOTimeZoneToLong(incomingDate)){
									hmCollectedUIds.put(lUID, incomingDate);
								}
							} else {
								hmCollectedUIds.put(lUID, incomingDate);
							}
						}
						
						collectorDBI.addCounterDataToBatch(callableStmt, uaCounterDataBean, lUID);
						
						if ((count + 1) % 1000 == 0 && (count + 1) >= 1000) {
							long dbStartTime = System.currentTimeMillis();
							insertedCount = collectorDBI.insertCounterDatasBatch(callableStmt);
							long dbTotalTime = System.currentTimeMillis() - dbStartTime; 
							if (dbTotalTime > 1000) {
								LogManager.infoLog("UnifiedCollectorDBInsTimeForSize:1000Statement:"+dbTotalTime);
								//Delay to ensure process in DB freed up
								Thread.sleep(500);
							}
							remainingData = remainingData - 1000;
						} 
						
					} else {
						LogManager.errorLog("Given GUID is not matching: " +uaCounterDataBean.getCounterData().getString("guid"));
					}
				}
				if (remainingData > 0 ){
					long dbStartTime = System.currentTimeMillis();
					insertedCount = collectorDBI.insertCounterDatasBatch(callableStmt);
					//collectorDBI.insertTest(conPC, uaCounterDataBean, lUID);
					long dbTotalTime = System.currentTimeMillis() - dbStartTime;
					//LogManager.infoLog("collectorDBInsTime:"+dbTotalTime+"|size:"+remainingData);
					if (dbTotalTime > 1000) {
						LogManager.infoLog("UnifiedCollectorDBInsTimeForSize:"+remainingData+"Statement:"+dbTotalTime);
						//Delay to ensure process in DB freed up
						Thread.sleep(500);
					}
				}
				
				// updates uid's last_appedo_received_on
				if (hmCollectedUIds.size() > 0 ) {
					pstmt = collectorDBI.initiateUpdateModuleLastReceivedUpdate(conPC);
					for (Map.Entry<Long, String> collectedUID : hmCollectedUIds.entrySet()) {
						collectorDBI.addLastReceivedOnDataToBatch(pstmt, collectedUID.getKey(), collectedUID.getValue());
					}
					long dbStartTime = System.currentTimeMillis();
					collectorDBI.insertLastReceivedBatch(pstmt);
					long dbTotalTime = System.currentTimeMillis() - dbStartTime;
					if (dbTotalTime > 1000) {
						Thread.sleep(500);
					}
				}
				
				if (hsUserIds.size() > 0 ) {
					long dbStartTime = System.currentTimeMillis();
					collectorDBI.updateBreachedUsers(conPC, hsUserIds.toString().replace("[","(").replace("]", ")"));
					long dbTotalTime = System.currentTimeMillis() - dbStartTime;
					if (dbTotalTime > 1000) {
						Thread.sleep(500);
					}
				}
				
				break;
			}
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			UtilsFactory.clearCollectionHieracy(uaCounterDataBean);
			uaCounterDataBean = null;
			
			DataBaseManager.close(callableStmt);
			callableStmt = null;
			
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			DataBaseManager.close(conPC);
			conPC = null;
		}
		 
	}
	
	private Long getUID(String strEncryptedUID) throws Exception {
		Long lUID = null;
		CollectorDBI collectorDBI = null;
		
		try{
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			collectorDBI = new CollectorDBI();
			
			lUID = collectorDBI.getModuleUID(conPC, strEncryptedUID);
		} catch ( Throwable th ) {
			throw th;
		} finally {
			collectorDBI = null;
		}
		
		return lUID;
	}
	
	private JSONObject getModuleDetail(String strEncryptedUID) throws Exception {
		JSONObject joModuleDetails = null;
		CollectorDBI collectorDBI = null;
		
		try{
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			collectorDBI = new CollectorDBI();
			
			joModuleDetails = collectorDBI.getModuleDetails(conPC, strEncryptedUID);
		} catch ( Throwable th ) {
			throw th;
		} finally {
			collectorDBI = null;
		}
		
		return joModuleDetails;
	}
	
	/*public static void main(String args[]) throws Exception {
		Set check = new HashSet<>();
		check.add("2");
		check.add("3");
		check.add("2");
		
		System.out.println(check.toString());
		System.out.println("select * from "+check.toString()+" ehckk ");
		
		List checkArray = new ArrayList<>(check); 
		
		System.out.println("select * frommm "+check.toString().replace("[","(").replace("]", ")")+" ehckk ");
		
		
	}*/
}
