package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.JavaProfilerDBI;
import com.appedo.webcollector.webserver.util.Constants.PROFILER_KEY;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * This is Java Profiler related manager class.
 * This will queue the profile-dataset into batch and insert them into database.
 * 
 * @author Ramkumar
 *
 */
public class JavaProfilerManager {
	
	// Database Connection object. Single connection will be maintained for entire JavaProfiler operations.
	private Connection conJavaProfiler = null;
		
	// DAO 
	private JavaProfilerDBI javaProfilerDBI = null;
	
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	public JavaProfilerManager() {

		establishDBonnection();
		
		javaProfilerDBI = new JavaProfilerDBI();
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	private void establishDBonnection() {
		try{
			conJavaProfiler = DataBaseManager.giveConnection();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
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
			conJavaProfiler = DataBaseManager.reEstablishConnection(conJavaProfiler);
			
			javaProfilerDBI.initializeInsertBatch(conJavaProfiler);
			
			// take one-by-one counter from the queue; and it to the db-batch.
			// on reaching 100 counters in the batch or when the queue is empty stop the loop.
			//System.out.println("JavaProfiler adding into batch...");
			addCounterBatch(strCounterEntry, lDistributorThreadId);
			
			// once DBI work is over, close the Statements
			javaProfilerDBI.clearPreparedStatement();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(conJavaProfiler);
			conJavaProfiler = null;
		}
	}
	
	/**
	 * Add the profile-dataset received to the queue, along with current time to keep track of received time.
	 * 
	 * @param strJavaProfiler
	 * @throws Exception
	 */
	private void addCounterBatch(String strJavaProfiler, long lDistributorThreadId) throws Throwable {
		JSONObject joJavaProfilerEntry = JSONObject.fromObject(strJavaProfiler), joPro = null;
		JSONArray jaJavaProfilerArray = null;
		
		//long lAgentResponseId = 0;
		
		long lCallerId = -1, lCalleeId = -1;
		String strRefererURI = null;
		
		// if connection is not established to db server then wait for 10 seconds
		conJavaProfiler = DataBaseManager.reEstablishConnection(conJavaProfiler);
		
		long lUID = (new CollectorDBI()).getModuleUID(conJavaProfiler, joJavaProfilerEntry.getString("1001"));
		if( lUID == -1l ) {
			throw new Exception("Given GUID is not matching: "+joJavaProfilerEntry.getString("1001"));
		}
		
		//lAgentResponseId = javaProfilerDBI.insertAgentResponse(lUID);
		
		// loop through each profiler entry
		jaJavaProfilerArray = joJavaProfilerEntry.getJSONArray("profilerArray");
		for( int i=0; i<jaJavaProfilerArray.size(); i++ ){
			joPro = jaJavaProfilerArray.getJSONObject(i);
			
			if( ! joPro.containsKey(PROFILER_KEY.REQUEST_URI.toString()) ){ joPro.put(PROFILER_KEY.REQUEST_URI.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.CLASS_NAME.toString()) ){ joPro.put(PROFILER_KEY.CLASS_NAME.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.METHOD_NAME.toString()) ){ joPro.put(PROFILER_KEY.METHOD_NAME.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.METHOD_SIGNATURE.toString()) ){ joPro.put(PROFILER_KEY.METHOD_SIGNATURE.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.QUERY.toString()) ){ joPro.put(PROFILER_KEY.QUERY.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.EXCEPTION_TYPE.toString()) ){ joPro.put(PROFILER_KEY.EXCEPTION_TYPE.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.EXCEPTION_MESSAGE.toString()) ){ joPro.put(PROFILER_KEY.EXCEPTION_MESSAGE.toString(), ""); }
			if( ! joPro.containsKey(PROFILER_KEY.EXCEPTION_STACKTRACE.toString()) ){ joPro.put(PROFILER_KEY.EXCEPTION_STACKTRACE.toString(), ""); }
			
			if( joPro.containsKey(PROFILER_KEY.CALLER_METHOD_ID.toString()) ){
				lCallerId = joPro.getLong(PROFILER_KEY.CALLER_METHOD_ID.toString());
			} else {
				lCallerId = -1;
			}
			if( joPro.containsKey(PROFILER_KEY.CALLEE_METHOD_ID.toString()) ){
				lCalleeId = joPro.getLong(PROFILER_KEY.CALLEE_METHOD_ID.toString());
			} else {
				lCalleeId = -1;
			}
			if( joPro.containsKey(PROFILER_KEY.REFERER_URI.toString()) ){
				strRefererURI = joPro.getString(PROFILER_KEY.REFERER_URI.toString());
			} else {
				strRefererURI = null;
			}
			
			
			// add one-by-one profiler line into batch
			javaProfilerDBI.addCounterBatch(lUID, joPro.getLong(PROFILER_KEY.THREAD_ID.toString()), joPro.getString(PROFILER_KEY.TYPE.toString()), 
					UtilsFactory.formatDateTimeToyyyyMMddHHmmss(joPro.getString(PROFILER_KEY.START_TIME.toString())), joPro.getInt(PROFILER_KEY.DURATION_MS.toString()), 
					joPro.getLong(PROFILER_KEY.APPROX_NANO_SEC_START.toString()), joPro.getLong(PROFILER_KEY.DURATION_NS.toString()), 
					strRefererURI, joPro.getString(PROFILER_KEY.REQUEST_URI.toString()), joPro.getString(PROFILER_KEY.LOCALHOST_NAME_IP.toString()), 
					joPro.getString(PROFILER_KEY.CLASS_NAME.toString()), joPro.getString(PROFILER_KEY.METHOD_NAME.toString()), joPro.getString(PROFILER_KEY.METHOD_SIGNATURE.toString()), 
					lCallerId, lCalleeId, 
					joPro.getString(PROFILER_KEY.QUERY.toString()), 
					joPro.getString(PROFILER_KEY.EXCEPTION_TYPE.toString()), joPro.getString(PROFILER_KEY.EXCEPTION_MESSAGE.toString()), joPro.getString(PROFILER_KEY.EXCEPTION_STACKTRACE.toString()) );
			
			if( javaProfilerDBI.getBatchCount() > 5000 || i == jaJavaProfilerArray.size()-1 ){
				// insert the counter as batch
				executeCounterBatch(lDistributorThreadId);
			}
			
			UtilsFactory.clearCollectionHieracy(joPro);
			joPro = null;
		}
		
		UtilsFactory.clearCollectionHieracy(joJavaProfilerEntry);
		joJavaProfilerEntry = null;
		UtilsFactory.clearCollectionHieracy(jaJavaProfilerArray);
		jaJavaProfilerArray = null;
		UtilsFactory.clearCollectionHieracy(joPro);
		joPro = null;
		
		strRefererURI = null;
		lCallerId = 0;
		lCalleeId = 0;
		lUID = 0l;
	}
	
	/**
	 * Execute the batched counter-set inserts
	 * 
	 * @throws Exception
	 */
	private void executeCounterBatch(long lDistributorThreadId) throws Exception {
		javaProfilerDBI.executeCounterBatch(lDistributorThreadId);
	}
	
	@Override
	protected void finalize() throws Throwable {
		
		// close the connection before this object is destory from JVM
		DataBaseManager.close(conJavaProfiler);
		
		super.finalize();
	}
}
