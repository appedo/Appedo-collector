package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.MSSQLSlowQueryDBI;
import com.appedo.webcollector.webserver.util.Constants.SLOW_QUERY;

/**
 * This is MSSQL Slow Query related manager class.
 * This will queue the MSSQL Slow Query dataset into batch and insert them into database.
 * 
 * @author Ramkumar R
 *
 */
public class MSSQLSlowQueryManager {
	
	// Database Connection object. Single connection will be maintained for entire MSSQL Slow Query operations.
	private Connection conMSSQLSlowQuery = null;
	
	// DAO 
	private MSSQLSlowQueryDBI mssqlSlowQueryDBI = null;
	
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	public MSSQLSlowQueryManager() {
		
		establishDBonnection();
		
		mssqlSlowQueryDBI = new MSSQLSlowQueryDBI();
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	private void establishDBonnection() {
		try{
			conMSSQLSlowQuery = DataBaseManager.giveConnection();
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	/**
	 * Retrieves the MSSQL Slow Query datasets from the respective queue.
	 * And inserts them in the database.
	 * 
	 * @throws Exception
	 */
	public void fetchCounter(String strCounterEntry, long lDistributorThreadId) throws Exception {
		
		try{
			// if connection is not established to db server then wait for 10 seconds
			conMSSQLSlowQuery = DataBaseManager.reEstablishConnection(conMSSQLSlowQuery);
			
			mssqlSlowQueryDBI.initializeInsertBatch(conMSSQLSlowQuery);
			
			// take one-by-one counter from the queue; and it to the db-batch.
			// on reaching 100 counters in the batch or whn the queue is empty stop the loop.
			//System.out.println("MSSQL Slow Query adding into batch...");
			execute(strCounterEntry, lDistributorThreadId);
			
			// once DBI work is over, close the Statements
			mssqlSlowQueryDBI.clearPreparedStatement();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(conMSSQLSlowQuery);
			conMSSQLSlowQuery = null;
		}
	}
	
	/**
	 * Add the dataset received to the queue, along with current time to keep track of received time.
	 * 
	 * It was addCounterBatch(); and batch insert is commented. 
	 * 
	 * @param strCounterEntry
	 * @param lDistributorThreadId
	 * @throws Exception
	 */
	private void execute(String strCounterEntry, long lDistributorThreadId) throws Exception {
		JSONObject joMSSQLSlowQueryEntry = JSONObject.fromObject(strCounterEntry), joPro = null;
		JSONArray jaMSSQLSlowQueryArray = null;
		
		conMSSQLSlowQuery = DataBaseManager.reEstablishConnection(conMSSQLSlowQuery);
		
		long lUID = (new CollectorDBI()).getModuleUID(conMSSQLSlowQuery, joMSSQLSlowQueryEntry.getString("1001"));
		if( lUID == -1l ) {
			throw new Exception("Given GUID is not matching: "+joMSSQLSlowQueryEntry.getString("1001"));
		}
		
		// loop through each  entry
		jaMSSQLSlowQueryArray = joMSSQLSlowQueryEntry.getJSONArray("slowQueries");
		for( int i=0; i<jaMSSQLSlowQueryArray.size(); i++ ){
			joPro = jaMSSQLSlowQueryArray.getJSONObject(i);
			
			if( ! joPro.containsKey(SLOW_QUERY.QUERY.toString()) ){ joPro.put(SLOW_QUERY.QUERY.toString(), ""); }
			if( ! joPro.containsKey(SLOW_QUERY.CALLS.toString()) ){ joPro.put(SLOW_QUERY.CALLS.toString(), ""); }
			if( ! joPro.containsKey(SLOW_QUERY.DURATION_MS.toString()) ){ joPro.put(SLOW_QUERY.DURATION_MS.toString(), ""); }
			
			try{
				// add one-by-one MSSQL Slow Query line into batch
				// mssqlSlowQueryDBI.addCounterBatch	// Required for batch insert; Batch process is not done as the duplicate queries will return exception
				mssqlSlowQueryDBI.execute(conMSSQLSlowQuery, lUID, joPro.getString(SLOW_QUERY.QUERY.toString()), 
						joPro.getLong(SLOW_QUERY.CALLS.toString()), joPro.getLong(SLOW_QUERY.DURATION_MS.toString()));
			
				/* Required for batch insert; Batch process is not done as the duplicate queries will return exception
				if( mssqlSlowQueryDBI.getBatchCount() > 100 || i == jaMSSQLSlowQueryArray.size()-1 ){
					// insert the counter as batch
					executeCounterBatch(lDistributorThreadId);
				}
				*/
			} catch(Exception ex) {
				LogManager.errorLog(ex);
			}
		}
	}
	
	/**
	 * Execute the batched counter-set inserts
	 * 
	 * @throws Exception
	 *
	private void executeCounterBatch(long lDistributorThreadId) throws Exception {
		mssqlSlowQueryDBI.executeCounterBatch(lDistributorThreadId);
	}*/
}
