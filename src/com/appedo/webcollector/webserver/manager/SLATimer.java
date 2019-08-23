package com.appedo.webcollector.webserver.manager;

import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;

import com.appedo.manager.LogManager;

/**
 * This is a timer class to invoke the SLA monito operation for the given interval.
 * 
 * @author Ramkumar R
 *
 */
public class SLATimer extends TimerTask {
	
	/**
	 * Get current time and add 5 seconds, to start the profiler db insert process
	 * 
	 * @return
	 */
	public static Date getFirstRunTime() throws Exception {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.add(Calendar.HOUR, 1);
		
		LogManager.infoLog("SLA timer thread will start on "+calendar.getTime());
		
		return calendar.getTime();
	}
	
	/**
	 * Invoke the SLA monitor operation.
	 * 
	 */
	public void run() {
		try{
			//System.out.println("Starting SLA timer thread: "+(new Date()));
			
			SLAManager.getSLAManager().monitorSLA();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("SLA timer thread stopped");
		
		super.finalize();
	}
}
