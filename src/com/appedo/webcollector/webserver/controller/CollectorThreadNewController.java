package com.appedo.webcollector.webserver.controller;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.CollectorBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.ModuleWriter;
import com.appedo.webcollector.webserver.util.Constants;

public class CollectorThreadNewController extends Thread {

	long lThisThreadId = 0l;
	
	private CollectorManager collectorManager = null;
	
	public CollectorThreadNewController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	

	@Override
	public void run() {
		//Timer timer = null;
		ModuleWriter moduleWriter = null;
		CollectorBean cbCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				//cbCounterEntry = collectorManager.pollCounter();
				cbCounterEntry = collectorManager.pollJSONCounter();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( cbCounterEntry != null ) {
					System.out.println("Counter Thread is Started!");
					moduleWriter = new ModuleWriter(cbCounterEntry, lThisThreadId);
					moduleWriter.start();
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
				moduleWriter = null;
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
