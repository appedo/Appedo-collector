package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;

import net.sf.json.JSONArray;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.CollectorParentDBI;
import com.appedo.webcollector.webserver.bean.NotificationBean;
import com.appedo.webcollector.webserver.util.UtilsFactory;

public class NotificationDataManager {
	
protected Connection conPC = null;
	
	protected CollectorParentDBI collectorDBI = null;
	
	protected CollectorManager collectorManager = null;
	
	public NotificationDataManager() throws Exception {
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
	
	public void fetchData(JSONArray jaNotificationData, long lThisThreadId) throws Exception {
		
		NotificationBean notificationBean = null;
		
		PreparedStatement pstmt = null;
		int remainingData = 0;
		long lSystemId = -1L; 
		
		try {
			notificationBean = new NotificationBean();
			while (true) {
				conPC = DataBaseManager.reEstablishConnection(conPC);
				
				pstmt = collectorDBI.initiateNotificationPreparedStmt(conPC);
				remainingData = jaNotificationData.size(); 
				for (int count = 0; count < jaNotificationData.size(); count++) {
					notificationBean.setNotificationData(jaNotificationData.getJSONObject(count));
					lSystemId = getSystemId(notificationBean.getNotificationData().getJSONObject("notificationData").getString("system_uuid"));
					if (lSystemId > 0) {
						collectorDBI.addNotificationDataBatch(pstmt, notificationBean, lSystemId);
						
						if ((count + 1) % 1000 == 0 && (count + 1) >= 1000) {
							long dbStartTime = System.currentTimeMillis();
							collectorDBI.insertNotificationDataBatch(pstmt);
							long dbTotalTime = System.currentTimeMillis() - dbStartTime;
							if (dbTotalTime > 1000) {
								LogManager.infoLog("UnifiedNotificationDBInsTimeForSize:1000Statement:"+dbTotalTime);
								Thread.sleep(500);
							}
							remainingData = remainingData - 1000;
						} 
					} else {
						LogManager.errorLog("Given System UUID is not matching: " +notificationBean.getNotificationData().getJSONObject("notificationData").getString("system_uuid"));
					}
				}
				
				if (remainingData > 0 ){
					long dbStartTime = System.currentTimeMillis();
					collectorDBI.insertNotificationDataBatch(pstmt);
					//collectorDBI.insertTest(conPC, uaCounterDataBean, lUID);
					long dbTotalTime = System.currentTimeMillis() - dbStartTime;
					if (dbTotalTime > 1000) {
						LogManager.infoLog("UnifiedNotificationDBInsTimeForSize:"+remainingData+"Statement:"+dbTotalTime);
						Thread.sleep(500);
					}
				}
				
				break;
			}
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			UtilsFactory.clearCollectionHieracy(notificationBean);
			notificationBean = null;
			
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			DataBaseManager.close(conPC);
			conPC = null;
		}
		
	}

	private Long getSystemId(String strUUID) throws Exception {
		Long lSystemId = -1L;
		CollectorDBI collectorDBI = null;
		
		try{
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			collectorDBI = new CollectorDBI();
			
			lSystemId = collectorDBI.getSystemId(conPC, strUUID);
		} catch ( Throwable th ) {
			throw th;
		} finally {
			collectorDBI = null;
		}
		
		return lSystemId;
	}
}
