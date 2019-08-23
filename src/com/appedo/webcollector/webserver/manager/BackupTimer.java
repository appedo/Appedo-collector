package com.appedo.webcollector.webserver.manager;

import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;

import com.appedo.manager.LogManager;

/**
 * Backup the counter tables.
 * Operations are Work-In-Process.
 * 
 * @author Ramkumar R
 *
 */
public class BackupTimer extends TimerTask {
	
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
		
	   // System.out.println("DBBackup timer thread(db insert) will start on "+calendar.getTime());
		
		return calendar.getTime();
	}
	
	public void run() {
		try{
			//System.out.println("Starting DBBackup timer thread(db insert): "+(new Date()));
			
			// TODO enable_backup
			//CollectorManager.getCollectorManager().backupTables();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("DBBackup timer thread stopped");
		
		super.finalize();
	}
}
