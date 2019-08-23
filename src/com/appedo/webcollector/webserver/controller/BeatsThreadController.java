package com.appedo.webcollector.webserver.controller;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.BeatsWriterTimer;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.util.Constants;

public class BeatsThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public BeatsThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
		BeatsWriterTimer beatsWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollBeatData();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					beatsWriterTimer = new BeatsWriterTimer(strCounterEntry, lThisThreadId);
					beatsWriterTimer.start();
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getBeatDataLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("BeatThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("BeatThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			} finally {
				beatsWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("BeatThreadController got stopped");
		LogManager.infoLog("BeatThreadController got stopped");
		LogManager.errorLog( new Exception("BeatThreadController got stopped") );
		super.finalize();
	}
}
