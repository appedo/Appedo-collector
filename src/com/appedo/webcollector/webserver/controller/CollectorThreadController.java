package com.appedo.webcollector.webserver.controller;

import com.appedo.webcollector.webserver.bean.CollectorBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.ModuleWriterTimer;
import com.appedo.webcollector.webserver.util.Constants;

public class CollectorThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	private CollectorManager collectorManager = null;
	
	public CollectorThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
		//Timer timer = null;
		ModuleWriterTimer moduleWriterTimer = null;
		CollectorBean cbCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				cbCounterEntry = collectorManager.pollCounter();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( cbCounterEntry != null ) {
					moduleWriterTimer = new ModuleWriterTimer(cbCounterEntry, lThisThreadId);
					moduleWriterTimer.start();
					//timer = new Timer();
					//timer.schedule(moduleWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getCounterLength();
				
				if( nQueueSize == 0 ) {
					LogManager.logControllerSleep("CollectorThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("CollectorThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Throwable th) {
				LogManager.errorLog(th);
			} finally {
				moduleWriterTimer = null;
//				timer = null;
				cbCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("CollectorThreadController got stopped");
		LogManager.infoLog("CollectorThreadController got stopped");
		LogManager.errorLog( new Exception("CollectorThreadController got stopped") );
		
		super.finalize();
	}
}
