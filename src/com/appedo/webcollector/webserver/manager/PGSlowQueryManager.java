package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.PGSlowQueryDBI;
import com.appedo.webcollector.webserver.util.Constants.SLOW_QUERY;

/**
 * This is .Net Profiler related manager class.
 * This will queue the profile-dataset into batch and insert them into database.
 * 
 * @author Ramkumar R
 *
 */
public class PGSlowQueryManager {
	
	// Database Connection object. Single connection will be maintained for entire .Net Profiler operations.
	private Connection conPGProfiler = null;
	
	// DAO 
	private PGSlowQueryDBI pgSlowQueryDBI = null;
	
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	public PGSlowQueryManager() {
		
		establishDBonnection();
		
		pgSlowQueryDBI = new PGSlowQueryDBI();
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	private void establishDBonnection() {
		try{
			conPGProfiler = DataBaseManager.giveConnection();
			
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
	public void fetchCounter(String strCounterEntry, long lDistributorThreadId) throws Exception {
		
		try{
			// if connection is not established to db server then wait for 10 seconds
			conPGProfiler = DataBaseManager.reEstablishConnection(conPGProfiler);
			
			pgSlowQueryDBI.initializeInsertBatch(conPGProfiler);
			
			// take one-by-one counter from the queue; and it to the db-batch.
			// on reaching 100 counters in the batch or whn the queue is empty stop the loop.
			//System.out.println(".Net Profiler adding into batch...");
			execute(strCounterEntry, lDistributorThreadId);
			
			// once DBI work is over, close the Statements
			pgSlowQueryDBI.clearPreparedStatement();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(conPGProfiler);
			conPGProfiler = null;
		}
	}
	
	/**
	 * Add the dataset received to the queue, along with current time to keep track of received time.
	 * 
	 * It was addCounterBatch(); and batch insert is commented.
	 * 
	 * @param strDotNetProfiler
	 * @throws Exception
	 */
	private void execute(String strCounterEntry, long lDistributorThreadId) throws Exception {
		JSONObject joPGSlowQueryEntry = JSONObject.fromObject(strCounterEntry), joPro = null;
		JSONArray jaPGSlowQueryArray = null;
		
		conPGProfiler = DataBaseManager.reEstablishConnection(conPGProfiler);
		
		long lUID = (new CollectorDBI()).getModuleUID(conPGProfiler, joPGSlowQueryEntry.getString("1001"));
		if( lUID == -1l ) {
			throw new Exception("Given GUID is not matching: "+joPGSlowQueryEntry.getString("1001"));
		}
		
		// loop through each entry
		jaPGSlowQueryArray = joPGSlowQueryEntry.getJSONArray("slowQueries");
		for( int i=0; i<jaPGSlowQueryArray.size(); i++ ){
			joPro = jaPGSlowQueryArray.getJSONObject(i);
			
			if( ! joPro.containsKey(SLOW_QUERY.QUERY.toString()) ){ joPro.put(SLOW_QUERY.QUERY.toString(), ""); }
			if( ! joPro.containsKey(SLOW_QUERY.CALLS.toString()) ){ joPro.put(SLOW_QUERY.CALLS.toString(), ""); }
			if( ! joPro.containsKey(SLOW_QUERY.DURATION_MS.toString()) ){ joPro.put(SLOW_QUERY.DURATION_MS.toString(), ""); }
				
			try{
				// add one-by-one profiler line into batch
				// pgSlowQueryDBI.addCounterBatch	// Required for batch insert; Batch process is not done as the duplicate queries will return exception
				pgSlowQueryDBI.execute(conPGProfiler, lUID, joPro.getString(SLOW_QUERY.QUERY.toString()), 
						joPro.getLong(SLOW_QUERY.CALLS.toString()), joPro.getLong(SLOW_QUERY.DURATION_MS.toString()));
				
				/* Required for batch insert; Batch process is not done as the duplicate queries will return exception
				if( pgSlowQueryDBI.getBatchCount() > 100 || i == jaPGSlowQueryArray.size()-1 ){
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
		pgSlowQueryDBI.executeCounterBatch(lDistributorThreadId);
	}*/
}
