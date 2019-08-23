package com.appedo.webcollector.webserver.manager;

import net.sf.json.JSONArray;

import com.appedo.manager.LogManager;

public class NotificationDataTimerWriter extends Thread {
	
	long lThisThreadId = 0, lDistributorThreadId = 0l;
	
	JSONArray jaNotificationData = null;
	
	public NotificationDataTimerWriter(JSONArray jaNotificationData, Long lDistributorThreadId) {
		this.jaNotificationData = jaNotificationData;
		this.lDistributorThreadId = lDistributorThreadId;
	}
	
	public void run() {
		try{
			lThisThreadId = Thread.currentThread().getId();
			
			LogManager.logWriterThreadLife("Starting NotificationDataTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaNotificationData);
			
			(new NotificationDataManager()).fetchData(jaNotificationData, lThisThreadId);
			
			LogManager.logWriterThreadLife("Completed NotificationDataTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaNotificationData);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("NotificationDataTimerWriter is destroyed "+lThisThreadId);
		
		super.finalize();
	}

}
