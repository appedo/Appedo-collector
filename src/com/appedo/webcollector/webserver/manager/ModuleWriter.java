package com.appedo.webcollector.webserver.manager;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.CollectorBean;

/**
 * Timer class to start the queue drain-insert operation for given interval.
 * 
 * @author Siddiq Ali A
 *
 */
public class ModuleWriter extends Thread {
	
	CollectorBean cbCounterEntry = null;
	
	long lThisThreadId = 0, lDistributorThreadId = 0l;
	
	public ModuleWriter(CollectorBean cbCounterEntry, Long lDistributorThreadId) {
		this.cbCounterEntry = cbCounterEntry;
		this.lDistributorThreadId = lDistributorThreadId;
	}
	
	/**
	 * Start the queue drain-insert operation.
	 * 
	 */
	public void run() {
		try{
			lThisThreadId = Thread.currentThread().getId();
			
			LogManager.logWriterThreadLife("Starting ModuleWriterTimer (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+cbCounterEntry);
			
			//(new ModulePerformanceCounterManager()).fetchCounter(cbCounterEntry, lThisThreadId);
			System.out.println("ModuleWriter Thread Started!");
			if(this.cbCounterEntry.getStrOldGUID() == null || this.cbCounterEntry.getStrOldGUID().equals("") ){
				System.out.println("New GUID Flow!");
				//(new ModulePerformanceCounterManager()).insertAndCreateModules(cbCounterEntry, lThisThreadId);
			}else{
				System.out.println("old GUID is Flow!");
				(new ModulePerformanceCounterManager()).migrationCounterModules(cbCounterEntry, lThisThreadId);
			}
			//(new ModulePerformanceCounterManager()).insertAndCreateModules(cbCounterEntry, lThisThreadId);
			
			LogManager.logWriterThreadLife("Completed ModuleWriterTimer (db insert) "+lThisThreadId+" <> Distributor: "+lDistributorThreadId+" <> Data: "+cbCounterEntry);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		LogManager.logWriterThreadLife("ModuleWriterTimer is destroyed "+lThisThreadId);
		
		super.finalize();
	}
}
