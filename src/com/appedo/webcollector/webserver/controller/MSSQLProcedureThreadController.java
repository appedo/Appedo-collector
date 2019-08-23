package com.appedo.webcollector.webserver.controller;

import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.DotNetProfilerWriterTimer;
import com.appedo.webcollector.webserver.manager.MSSQLProcedureWriterTimer;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;

public class MSSQLProcedureThreadController extends Thread {
	
	long lThisThreadId = 0l;
	
	protected CollectorManager collectorManager = null;
	
	public MSSQLProcedureThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
//		Timer timer = null;
		MSSQLProcedureWriterTimer mssqlProcedureWriterTimer = null;
		String strCounterEntry = null;
		
		lThisThreadId = Thread.currentThread().getId();
		
		while(true) {
			try{
				strCounterEntry = collectorManager.pollMssqlProcedures();
				
				// take one-by-one counter from the queue; and add it to the db-batch.
				if( strCounterEntry != null ){
					mssqlProcedureWriterTimer = new MSSQLProcedureWriterTimer(strCounterEntry, lThisThreadId);
					mssqlProcedureWriterTimer.start();
//					timer = new Timer();
//					timer.schedule(dotNetProfilerWriterTimer, 5l);
				}
				
				// if the above loop was stopped as queue was empty then wait for 30 seconds.
				int nQueueSize = collectorManager.getMSSQLProcedureLength();
				
				if( nQueueSize == 0 ){
					LogManager.logControllerSleep("MSSQLProcedureThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					LogManager.logControllerSleep("MSSQLProcedureThreadController is wokeup...");
				}
				
				nQueueSize = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			} finally {
				mssqlProcedureWriterTimer = null;
//				timer = null;
				strCounterEntry = null;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("MSSQLProcedureThreadController got stopped");
		LogManager.infoLog("MSSQLProcedureThreadController got stopped");
		LogManager.errorLog( new Exception("MSSQLProcedureThreadController got stopped") );
		
		super.finalize();
	}
}
