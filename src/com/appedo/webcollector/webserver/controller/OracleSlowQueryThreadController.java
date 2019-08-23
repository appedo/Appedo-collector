package com.appedo.webcollector.webserver.controller;

import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.OracleSlowQueryWriterTimer;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;

public class OracleSlowQueryThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public OracleSlowQueryThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
//		Timer timer = null;
		OracleSlowQueryWriterTimer oracleSlowQueryWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollOracleSlowQueries();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					oracleSlowQueryWriterTimer = new OracleSlowQueryWriterTimer(strCounterEntry, lThisThreadId);
					oracleSlowQueryWriterTimer.start();
//					timer = new Timer();
//					timer.schedule(dotNetProfilerWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getOracleSlowQueriesLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("OracleSlowQueryThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("OracleSlowQueryThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			} finally {
				oracleSlowQueryWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("OracleSlowQueryThreadController got stopped");
		LogManager.infoLog("OracleSlowQueryThreadController got stopped");
		LogManager.errorLog( new Exception("OracleSlowQueryThreadController got stopped") );
		
		super.finalize();
	}
}
