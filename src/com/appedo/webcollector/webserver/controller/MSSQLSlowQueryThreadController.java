package com.appedo.webcollector.webserver.controller;

import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.DotNetProfilerWriterTimer;
import com.appedo.webcollector.webserver.manager.MSSQLSlowQueryWriterTimer;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;

public class MSSQLSlowQueryThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public MSSQLSlowQueryThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
//		Timer timer = null;
		MSSQLSlowQueryWriterTimer mssqlSlowQueryWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollMssqlSlowQueries();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					mssqlSlowQueryWriterTimer = new MSSQLSlowQueryWriterTimer(strCounterEntry, lThisThreadId);
					mssqlSlowQueryWriterTimer.start();
//					timer = new Timer();
//					timer.schedule(dotNetProfilerWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getMSSQLSlowQueriesLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("MSSQLSlowQueryThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("MSSQLSlowQueryThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			} finally {
				mssqlSlowQueryWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("MSSQLSlowQueryThreadController got stopped");
		LogManager.infoLog("MSSQLSlowQueryThreadController got stopped");
		LogManager.errorLog( new Exception("MSSQLSlowQueryThreadController got stopped") );
		
		super.finalize();
	}
}
