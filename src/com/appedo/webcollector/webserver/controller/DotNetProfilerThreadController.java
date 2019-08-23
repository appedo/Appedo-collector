package com.appedo.webcollector.webserver.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.DotNetProfilerManager;
import com.appedo.webcollector.webserver.manager.DotNetProfilerWriterTimer;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;

public class DotNetProfilerThreadController extends Thread {
	
	long lThisThreadId = 0l;
	String TYPE = null;
	
	protected CollectorManager collectorManager = null;
	
	public DotNetProfilerThreadController(String type) {
		collectorManager = CollectorManager.getCollectorManager();
		
		this.TYPE = type;
	}
	
	@Override
	public void run() {
		DotNetProfilerWriterTimer dotNetProfilerWriterTimer = null;
		DotNetProfilerManager dotNetProfilerManager = new DotNetProfilerManager();
		
		ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > alComputedMethodTraces = null;
		lThisThreadId = Thread.currentThread().getId();
		
		Date dateStart = null;
		
		while(true) {
			dateStart = LogManager.logMethodStart();
			
			if( TYPE.equals("STACKTRACE_COMPUTE") ) {
				try{
					dotNetProfilerManager.establishDBConnection();
					
					// One GUID has to be worked by only one of the ControllerThread.
					// So get-&-lock a GUID. For the locked GUID, get the Method Traces.
					dotNetProfilerManager.getNextComputeTraceDetails();
					
					// If a GUID is locked for this Thread, then start building the Data-Structure with Stack & HashTables
					if( dotNetProfilerManager.getUID() != null ){
						// TODO RAM, Separate the processing into another Thread.
						
						dotNetProfilerManager.buildStackTrace();
						
						// Once the Method Traces are processed, remove them from DB.
						dotNetProfilerManager.removeSummaryEntry();
						
						//System.out.println("Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+dotNetProfilerManager.getUID()+" <> Compute Process completed.");
						LogManager.infoLog("DNP <> Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+dotNetProfilerManager.getUID()+" <> Compute Process completed.");
					}
					
					dotNetProfilerManager.commitConnection();
				} catch(Throwable th) {
					LogManager.errorLog(th);
					
					LogManager.infoLog("DNP <> Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+dotNetProfilerManager.getUID()+" <> Compute Process completed with Exception.");
					
					// wait for few seconds
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						LogManager.errorLog(e);
					}
				} finally {
					dotNetProfilerManager.closeConnection();
					
					LogManager.logMethodEnd(dateStart);
					
					try {
						LogManager.logControllerSleep("DotNetProfilerThreadController-STACKTRACE_COMPUTE is sleeping...");
						Thread.sleep( Constants.DOTNET_PROFILER_COMPUTE_REST_MILLESECONDS );
						LogManager.logControllerSleep("DotNetProfilerThreadController-STACKTRACE_COMPUTE is sleeping...");
					} catch (InterruptedException e) {
						LogManager.errorLog(e);
					}
				}
				
			} else if( TYPE.equals("STACKTRACE_INSERT") ) {
				try{
					alComputedMethodTraces = dotNetProfilerManager.getComputedGUID();
					
					// take one-by-one counter from the queue; and add it to the db-batch.
					if( alComputedMethodTraces != null && alComputedMethodTraces.size() > 0 ){
						
						dotNetProfilerWriterTimer = new DotNetProfilerWriterTimer(dotNetProfilerManager.getUID(), alComputedMethodTraces);
						dotNetProfilerWriterTimer.start();
//						timer = new Timer();
//						timer.schedule(dotNetProfilerWriterTimer, 5l);
					}
					
				} catch(Throwable th) {
					LogManager.errorLog(th);
					
					// wait for few seconds
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						LogManager.errorLog(e);
					}
				}  finally {
					dotNetProfilerWriterTimer = null;
					alComputedMethodTraces = null;
					
					LogManager.logMethodEnd(dateStart);
					
					try {
						LogManager.logControllerSleep("DotNetProfilerThreadController-STACKTRACE_INSERT is sleeping...");
						Thread.sleep( Constants.DOTNET_PROFILER_INSERT_REST_MILLESECONDS );
						LogManager.logControllerSleep("DotNetProfilerThreadController-STACKTRACE_INSERT is wokeup...");
					} catch (InterruptedException e) {
						LogManager.errorLog(e);
					}
				}
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("DotNetProfilerThreadController got stopped");
		LogManager.infoLog("DNP <> DotNetProfilerThreadController got stopped");
		LogManager.errorLog( new Exception("DotNetProfilerThreadController got stopped") );
		
		super.finalize();
	}
}
