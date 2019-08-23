package com.appedo.webcollector.webserver.manager;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;

/**
 * Timer class to start the queue drain-insert operation for given interval.
 * 
 * @author Ramkumar R
 *
 */
public class DotNetProfilerWriterTimer extends Thread {
	
	private DotNetProfilerManager dotNetProfilerManager = null;
	
	public DotNetProfilerWriterTimer(String strUID, ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > alComputedMethodTraces) {
		this.dotNetProfilerManager = new DotNetProfilerManager(strUID, alComputedMethodTraces);
	}
	
	/**
	 * Start the queue drain-insert operation.
	 * 
	 */
	public void run() {
		Date dateLog = null;
		dateLog = LogManager.logMethodStart();
		
		try{
			LogManager.logWriterThreadLife("DNP <> Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+dotNetProfilerManager.getUID()+" <> Insert Process started.");
			
			dotNetProfilerManager.establishDBConnection();
			
			dotNetProfilerManager.fetchData();
			
			dotNetProfilerManager.commitConnection();
			
			LogManager.logWriterThreadLife("DNP <> Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+dotNetProfilerManager.getUID()+" <> Insert Process completed.");
			
		} catch(Throwable e) {
			LogManager.errorLog(e);

			LogManager.logWriterThreadLife("DNP <> Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+dotNetProfilerManager.getUID()+" <> Insert Process completed with Exception.");
		} finally {
			dotNetProfilerManager.closeConnection();
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("DNP <> DotNetProfilerWriter thread destroyed "+Thread.currentThread().getId());
		
		super.finalize();
	}
}
