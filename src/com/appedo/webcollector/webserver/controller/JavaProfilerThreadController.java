package com.appedo.webcollector.webserver.controller;

import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.JavaProfilerWriterTimer;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;

public class JavaProfilerThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public JavaProfilerThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
//		Timer timer = null;
		JavaProfilerWriterTimer javaProfilerWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollJavaProfiler();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					javaProfilerWriterTimer = new JavaProfilerWriterTimer(strCounterEntry, lThisThreadId);
					javaProfilerWriterTimer.start();
//					timer = new Timer();
//					timer.schedule(javaProfilerWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getJavaProfilerLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("JavaProfilerThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("JavaProfilerThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Throwable th) {
				LogManager.errorLog(th);
			} finally {
				javaProfilerWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("JavaProfilerThreadController got stopped");
		LogManager.infoLog("JavaProfilerThreadController got stopped");
		LogManager.errorLog( new Exception("JavaProfilerThreadController got stopped") );
		
		super.finalize();
	}
}
