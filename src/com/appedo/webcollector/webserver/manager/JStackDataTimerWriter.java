package com.appedo.webcollector.webserver.manager;

import com.appedo.manager.LogManager;

import net.sf.json.JSONArray;

public class JStackDataTimerWriter extends Thread {
	
long lThisThreadId = 0, lDistributorThreadId = 0l;
	
	JSONArray jaJStackData = null;
	
	public JStackDataTimerWriter(JSONArray jaJStackData) {
		this.jaJStackData = jaJStackData;
	}
	
	public void run() {
		try{
			lThisThreadId = Thread.currentThread().getId();
			
			//LogManager.logWriterThreadLife("Starting JStackDataTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaCountersDatas);
			
			(new JStackDataManager()).fetchCounters(jaJStackData);
			
			//LogManager.logWriterThreadLife("Completed JStackDataTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaCountersDatas);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("JStackDataTimerWriter is destroyed "+lThisThreadId);
		
		super.finalize();
	}

}
