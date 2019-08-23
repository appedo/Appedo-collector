package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.MSSQLProcedureDBI;
import com.appedo.webcollector.webserver.util.Constants.SLOW_QUERY;

/**
 * This is MSSQL Slow Procedure related manager class.
 * This will queue the Slow Procedure dataset into batch and insert them into database.
 * 
 * @author Ramkumar R
 *
 */
public class MSSQLProcedureManager {
	
	// Database Connection object. Single connection will be maintained for entire MSSQL Slow Procedure operations.
	private Connection conMSSQLSlowProcedure = null;
	
	// DAO 
	private MSSQLProcedureDBI mssqlProcedureDBI = null;
	
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	public MSSQLProcedureManager() {
		
		establishDBonnection();
		
		mssqlProcedureDBI = new MSSQLProcedureDBI();
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	private void establishDBonnection() {
		try{
			conMSSQLSlowProcedure = DataBaseManager.giveConnection();
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	/**
	 * Retrieves the Slow Procedure datasets from the respective queue.
	 * And inserts them in the database.
	 * 
	 * @throws Exception
	 */
	public void fetchCounter(String strCounterEntry, long lDistributorThreadId) throws Exception {
		
		try{
			// if connection is not established to db server then wait for 10 seconds
			conMSSQLSlowProcedure = DataBaseManager.reEstablishConnection(conMSSQLSlowProcedure);
			
			mssqlProcedureDBI.initializeInsertBatch(conMSSQLSlowProcedure);
			
			// take one-by-one counter from the queue; and it to the db-batch.
			// on reaching 100 counters in the batch or whn the queue is empty stop the loop.
			//System.out.println("MSSQL Slow Procedure adding into batch...");
			execute(strCounterEntry, lDistributorThreadId);
			
			// once DBI work is over, close the Statements
			mssqlProcedureDBI.clearPreparedStatement();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(conMSSQLSlowProcedure);
			conMSSQLSlowProcedure = null;
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
		JSONObject joMSSQLProcedureEntry = JSONObject.fromObject(strCounterEntry), joPro = null;
		JSONArray jaMSSQLProcedureQueryArray = null;

		conMSSQLSlowProcedure = DataBaseManager.reEstablishConnection(conMSSQLSlowProcedure);
		
		long lUID = (new CollectorDBI()).getModuleUID(conMSSQLSlowProcedure, joMSSQLProcedureEntry.getString("1001"));
		if( lUID == -1l ) {
			throw new Exception("Given GUID is not matching: "+joMSSQLProcedureEntry.getString("1001"));
		}
		
		// loop through each  entry
		jaMSSQLProcedureQueryArray = joMSSQLProcedureEntry.getJSONArray("slowQueries");
		for( int i=0; i<jaMSSQLProcedureQueryArray.size(); i++ ){
			joPro = jaMSSQLProcedureQueryArray.getJSONObject(i);
			
			if( ! joPro.containsKey(SLOW_QUERY.QUERY.toString()) ){ joPro.put(SLOW_QUERY.QUERY.toString(), ""); }
			if( ! joPro.containsKey(SLOW_QUERY.CALLS.toString()) ){ joPro.put(SLOW_QUERY.CALLS.toString(), ""); }
			if( ! joPro.containsKey(SLOW_QUERY.DURATION_MS.toString()) ){ joPro.put(SLOW_QUERY.DURATION_MS.toString(), ""); }

			try{
				// add one-by-one Slow Procedure line into batch
				// mssqlProcedureDBI.addCounterBatch	// Required for batch insert; Batch process is not done as the duplicate queries will return exception
				mssqlProcedureDBI.execute(conMSSQLSlowProcedure, lUID, joPro.getString(SLOW_QUERY.QUERY.toString()), 
					joPro.getLong(SLOW_QUERY.CALLS.toString()), joPro.getLong(SLOW_QUERY.DURATION_MS.toString()));
			
				/* Required for batch insert; Batch process is not done as the duplicate queries will return exception
				if( mssqlProcedureDBI.getBatchCount() > 100 || i == jaMSSQLProcedureQueryArray.size()-1 ){
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
		mssqlProcedureDBI.executeCounterBatch(lDistributorThreadId);
	}*/
}
