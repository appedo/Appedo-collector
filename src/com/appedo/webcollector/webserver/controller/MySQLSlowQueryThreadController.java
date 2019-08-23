package com.appedo.webcollector.webserver.controller;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.MySQLSlowQueryWriterTimer;
import com.appedo.webcollector.webserver.util.Constants;

public class MySQLSlowQueryThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public MySQLSlowQueryThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
//		Timer timer = null;
		MySQLSlowQueryWriterTimer mysqlSlowQueryWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollMysqlSlowQueries();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					mysqlSlowQueryWriterTimer = new MySQLSlowQueryWriterTimer(strCounterEntry, lThisThreadId);
					mysqlSlowQueryWriterTimer.start();
//					timer = new Timer();
//					timer.schedule(dotNetProfilerWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getMySQLSlowQueriesLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("MySQLSlowQueryThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("MySQLSlowQueryThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			} finally {
				mysqlSlowQueryWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("MySQLSlowQueryThreadController got stopped");
		LogManager.infoLog("MySQLSlowQueryThreadController got stopped");
		LogManager.errorLog( new Exception("MySQLSlowQueryThreadController got stopped") );
		
		super.finalize();
	}
}
