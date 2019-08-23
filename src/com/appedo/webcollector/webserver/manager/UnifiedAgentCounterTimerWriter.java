package com.appedo.webcollector.webserver.manager;

import net.sf.json.JSONArray;

import com.appedo.manager.LogManager;

public class UnifiedAgentCounterTimerWriter extends Thread {

	long lThisThreadId = 0, lDistributorThreadId = 0l;
	
	JSONArray jaCountersDatas = null;

	public UnifiedAgentCounterTimerWriter(JSONArray jaCountersDatas) {
		this.jaCountersDatas = jaCountersDatas;
		//this.lDistributorThreadId = lDistributorThreadId;
	}
	
	public void run() {
		try{
			lThisThreadId = Thread.currentThread().getId();
			
			//LogManager.logWriterThreadLife("Starting UnifiedAgentCounterTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaCountersDatas);
			
			(new UnifiedAgentCounterManager()).fetchCounters(jaCountersDatas);
			
			//LogManager.logWriterThreadLife("Completed UnifiedAgentCounterTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaCountersDatas);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("UnifiedAgentCounterTimerWriter is destroyed "+lThisThreadId);
		
		super.finalize();
	}
}
