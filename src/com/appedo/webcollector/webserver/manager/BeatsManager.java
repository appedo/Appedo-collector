package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.BeatsDBI;

public class BeatsManager {
	
	// Database Connection object. Single connection will be maintained for entire .Net Profiler operations.
	private Connection conBeat = null;
	
	// DAO 
	private BeatsDBI beatsDBI = null;
	
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	public BeatsManager() {
		
		establishDBonnection();
		
		beatsDBI = new BeatsDBI();
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	private void establishDBonnection() {
		try{
			conBeat = DataBaseManager.giveConnection();
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	/**
	 * Retrieves the profile-datasets from the respective queue.
	 * And inserts them in the database.
	 * 
	 * @throws Exception
	 */
	public void updateBeatAgentTime(String strCounterEntry, long lDistributorThreadId) throws Exception {
		
		try{
			// if connection is not established to db server then wait for 10 seconds
			conBeat = DataBaseManager.reEstablishConnection(conBeat);
			
			beatsDBI.execute(conBeat,strCounterEntry);
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(conBeat);
			conBeat = null;
		}
	}

}
