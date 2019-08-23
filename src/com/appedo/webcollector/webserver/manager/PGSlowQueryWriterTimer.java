package com.appedo.webcollector.webserver.manager;

import com.appedo.manager.LogManager;

/**
 * Timer class to start the queue drain-insert operation for given interval.
 * 
 * @author Ramkumar R
 *
 */
public class PGSlowQueryWriterTimer extends Thread {
	
	private String strCounterEntry = null;
	
	long lThisThreadId = 0, lDistributorThreadId = 0l;
	
	public PGSlowQueryWriterTimer(String strCounterEntry, long lDistributorThreadId) {
		this.strCounterEntry = strCounterEntry;
		this.lDistributorThreadId = lDistributorThreadId;
	}
	
	/**
	 * Start the queue drain-insert operation.
	 * 
	 */
	public void run() {
		try{
			lThisThreadId = Thread.currentThread().getId();
			
			LogManager.logWriterThreadLife("Starting PGSlowQueryWriterTimer (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+strCounterEntry);
			
			(new PGSlowQueryManager()).fetchCounter(strCounterEntry, lDistributorThreadId);
			
			LogManager.logWriterThreadLife("Completed PGSlowQueryWriterTimer (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+strCounterEntry);
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("PGSlowQueryWriterTimer is destroyed "+lThisThreadId);
		
		super.finalize();
	}
}
