package com.appedo.webcollector.webserver.controller;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.PGSlowQueryWriterTimer;
import com.appedo.webcollector.webserver.util.Constants;

public class PGSlowQueryThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public PGSlowQueryThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
//		Timer timer = null;
		PGSlowQueryWriterTimer pgSlowQueryWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollPGSlowQueries();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					pgSlowQueryWriterTimer = new PGSlowQueryWriterTimer(strCounterEntry, lThisThreadId);
					pgSlowQueryWriterTimer.start();
//					timer = new Timer();
//					timer.schedule(dotNetProfilerWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getPGSlowQueriesLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("PGSlowQueryThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("PGSlowQueryThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			} finally {
				pgSlowQueryWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("PGSlowQueryThreadController got stopped");
		LogManager.infoLog("PGSlowQueryThreadController got stopped");
		LogManager.errorLog( new Exception("PGSlowQueryThreadController got stopped") );
		
		super.finalize();
	}
}
