package com.appedo.webcollector.webserver.manager;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.CollectorParentDBI;
import com.appedo.webcollector.webserver.DBI.JStackDBI;
import com.appedo.webcollector.webserver.bean.JStackDataBean;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JStackDataManager {
	
protected Connection conPC = null;
	
	protected JStackDBI jStackDBI = null;
	protected CollectorParentDBI collectorDBI = null;
	
	public JStackDataManager() throws Exception {
		
		jStackDBI = new JStackDBI();
		collectorDBI = new CollectorParentDBI();
		
		
		establishDBonnection();
	}
	
	protected void establishDBonnection(){
		try{
			conPC = DataBaseManager.giveConnection();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
	}
	
	public void fetchCounters(JSONArray jaJStackData) throws Exception {
		
		CallableStatement callableStmt = null;
		PreparedStatement pstmt = null;
		JStackDataBean jStackBean = null;
		int remainingData = 0;
		long lUID = -1L;
		int insertedCount = 0;
		JSONObject joJStackData = null;
		HashMap<Long, String> hmCollectedUIds = null;
		
		try {
			
			jStackBean = new JStackDataBean();
			while (true) {
				
				conPC = DataBaseManager.reEstablishConnection(conPC);
				
				hmCollectedUIds = new HashMap<Long, String>();
				
				callableStmt = jStackDBI.createJStackTablePreparedStmt(conPC);
				
				remainingData = jaJStackData.size();
				
				for (int count = 0; count < jaJStackData.size(); count++) {
					conPC = DataBaseManager.reEstablishConnection(conPC);
					
					joJStackData = jaJStackData.getJSONObject(count);
					lUID = getUID(joJStackData.getString("guid"));
					if (lUID > 0 ) {
						
						String incomingDate = joJStackData.getString("datetime");
						// if net stack data received, get UID & latest to update last_appedo_received_on in module_master
						if (hmCollectedUIds.containsKey(lUID)) {
							String existingDate = hmCollectedUIds.get(lUID);
							if(UtilsFactory.formatISOTimeZoneToLong(existingDate) < UtilsFactory.formatISOTimeZoneToLong(incomingDate)){
								hmCollectedUIds.put(lUID, incomingDate);
							}
						} else {
							hmCollectedUIds.put(lUID, incomingDate);
						}
						
						jStackDBI.addJStackDataToBatch(callableStmt, joJStackData, lUID);
						
						if ((count + 1) % 1000 == 0 && (count + 1) >= 1000) {
							long dbStartTime = System.currentTimeMillis();
							
							insertedCount = jStackDBI.insertJStackDataBatch(callableStmt);
							long dbTotalTime = System.currentTimeMillis() - dbStartTime; 
							/*System.out.println("--------------------------------------------------------------------");
							System.out.println("Start Time : "+dbStartTime+ "End Time : "+System.currentTimeMillis());
							System.out.println("--------------------------------------------------------------------");*/
							if (dbTotalTime > 1000) {
								LogManager.infoLog("JStackDataDBInsTimeForSize:1000Statement:"+dbTotalTime);
								//Delay to ensure process in DB freed up
								//Thread.sleep(500);
							}
							remainingData = remainingData - 1000;
						} 
					} else {
						LogManager.errorLog("Given GUID is not matching: " +joJStackData.getString("guid"));
					}
				}
				
				if (remainingData > 0 ){
					long dbStartTime = System.currentTimeMillis();
					insertedCount = jStackDBI.insertJStackDataBatch(callableStmt);
					/*System.out.println("--------------------------------------------------------------------");
					System.out.println("Start Time : "+dbStartTime+ "End Time : "+System.currentTimeMillis());
					System.out.println("--------------------------------------------------------------------");*/
					//collectorDBI.insertTest(conPC, uaCounterDataBean, lUID);
					long dbTotalTime = System.currentTimeMillis() - dbStartTime;
					//LogManager.infoLog("collectorDBInsTime:"+dbTotalTime+"|size:"+remainingData);
					if (dbTotalTime > 1000) {
						LogManager.infoLog("JStackDataDBInsTimeForSize:"+remainingData+"Statement:"+dbTotalTime);
						//Delay to ensure process in DB freed up
						//Thread.sleep(500);
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
				
				//break the while loop once execution completes.
				break;
			}
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			UtilsFactory.clearCollectionHieracy(jStackBean);
			jStackBean = null;
			
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

}
