package com.appedo.webcollector.webserver.manager;

import com.appedo.manager.LogManager;

import net.sf.json.JSONArray;

public class NetStackDataTimerWriter extends Thread {
	
	long lThisThreadId = 0, lDistributorThreadId = 0l;
	
	JSONArray jaNetStackData = null;
	
	public NetStackDataTimerWriter(JSONArray jaNetStackData) {
		this.jaNetStackData = jaNetStackData;
	}

	public void run() {
		try{
			lThisThreadId = Thread.currentThread().getId();
			
			//LogManager.logWriterThreadLife("Starting NetStackDataTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaCountersDatas);
			
			(new NetStackDataManager()).fetchCounters(jaNetStackData);
			
			//LogManager.logWriterThreadLife("Completed NetStackDataTimerWriter (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+jaCountersDatas);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("NetStackDataTimerWriter is destroyed "+lThisThreadId);
		
		super.finalize();
	}
}
