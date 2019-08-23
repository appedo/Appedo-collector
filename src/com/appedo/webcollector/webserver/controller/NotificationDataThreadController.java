package com.appedo.webcollector.webserver.controller;

import java.util.ArrayList;

import net.sf.json.JSONArray;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.NotificationBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.NotificationDataTimerWriter;
import com.appedo.webcollector.webserver.util.Constants;

public class NotificationDataThreadController extends Thread {
	long lThisThreadId = 0l;
	
	private CollectorManager collectorManager = null;
	public NotificationDataThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
		
	}
	
	public void run() {
		NotificationDataTimerWriter notificationDataTimerWriter = null;
		JSONArray jaNotificationData = null;
		ArrayList<NotificationBean> alNotificationDataBean = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try {
				//Constants.NOTIFICATION_DATA_RUNNING_TIME = System.currentTimeMillis();
				int nQueueSize = collectorManager.getNotificationDataSize();
				//System.out.println("current Time : "+ currentTime+", notification timer :"+Constants.NOTIFICATION_DATA_TIMER_CHECK);
				/*if (Constants.NOTIFICATION_DATA_RUNNING_TIME - Constants.NOTIFICATION_DATA_TIMER_CHECK >=10000) {
					//System.out.println("notification time cleared");
					LogManager.infoLog("NotificationQueueSize:"+nQueueSize);
					Constants.NOTIFICATION_DATA_TIMER_CHECK = System.currentTimeMillis();
				}*/
				
				if( nQueueSize == 0 ) {
					LogManager.logControllerSleep("NotificationDataThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("NotificationDataThreadController is wokeup...");
					continue;
				}
				nQueueSize = 0;
				
				alNotificationDataBean = collectorManager.pollNotificationData();
				
				if(alNotificationDataBean != null && alNotificationDataBean.size() > 0) {
					jaNotificationData = JSONArray.fromObject(alNotificationDataBean);
					notificationDataTimerWriter = new NotificationDataTimerWriter(jaNotificationData, lThisThreadId);
					notificationDataTimerWriter.start();
				}
			} catch(Throwable th) {
				LogManager.errorLog(th);
			} finally {
				notificationDataTimerWriter = null;
				alNotificationDataBean = null;
				jaNotificationData = null;
			}
		}
	}

}
