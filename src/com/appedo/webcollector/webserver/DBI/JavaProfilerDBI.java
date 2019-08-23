package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * Java Profiler related Database Interface layer.
 * This do the operations related to the Java Profiler records table.
 * 
 * @author Ramkumar R
 *
 */
public class JavaProfilerDBI {
	
	private String strProfilerQry = null;
	
	private Statement stmtJavaProfiler = null;
	private PreparedStatement pstmtPartitionTables = null;
	
	private int nBatchCount = 0;
	
	private String strUID = "";
	
	/**
	 * Initializes the prepared statements with the respective insert queries.
	 * The prepared statements will be then used in the queuing and execution process
	 * 
	 * @param con
	 * @throws Exception
	 */
	public void initializeInsertBatch(Connection con) {
		
		try{
			// For each counter in the received agent response; with a reference for above entry.
			strProfilerQry = "INSERT INTO tomcat_profiler_@KEY@ (uid, appedo_received_on, thread_id, type, start_time, duration_ms, approx_nano_sec_start, duration_ns, referer_uri, request_URI, localhost_name_ip, class_name, method_name, method_signature, query, caller_method_id, callee_method_id, exception_type, exception_message, exception_stacktrace) VALUES ";
			
			stmtJavaProfiler = con.createStatement();
			
			pstmtPartitionTables = con.prepareStatement("SELECT create_asd_daily_partition_tables(?, ?, false, false, false, true)");
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	/**
	 * Insert the one-time response entry for each profiler-set when received.
	 * 
	 * @param lUID
	 * @return
	 * @throws Exception
	 *
	public long insertAgentResponse(long lUID) throws Exception {
		long lReturn = 0;
		
		try{
			psAgentResponse.setLong(1, lUID);
			
			lReturn = DataBaseManager.insertAndReturnKey(psAgentResponse, "response_id");
			
		} catch(Exception ex) {
			System.out.println("Exception in insertAgentResponse: "+ex.getMessage());
			throw ex;
		}
		
		return lReturn;
	}*/
	
	/**
	 * Queue the Profiler entry into the batch. The batch will be executed when the executeCounterBatch is called.
	 * 
	 * @param lAgentResponseId
	 * @param lThreadId
	 * @param strType
	 * @param strStartTime
	 * @param nDurationInMilliSec
	 * @param lApproxStartTimeInNanoSec
	 * @param duration_ns
	 * @param strRefererURI
	 * @param strRequestURI
	 * @param strLocalHostnameIP
	 * @param strClassName
	 * @param strMethodName
	 * @param strMethodSignature
	 * @param strQuery
	 * @param lCallerMethodId
	 * @param lCalleeMethodId
	 * @param strExceptionType
	 * @param strExceptionMessage
	 * @param strExceptionStackTrace
	 * @throws Exception
	 */
	public void addCounterBatch(long lUID, long lThreadId, String strType, String strStartTime, int nDurationInMilliSec, long lApproxStartTimeInNanoSec, long duration_ns, String strRefererURI, String strRequestURI, String strLocalHostnameIP, String strClassName, String strMethodName, String strMethodSignature, long lCallerMethodId, long lCalleeMethodId, String strQuery, String strExceptionType, String strExceptionMessage, String strExceptionStackTrace) throws Exception {
		StringBuilder sbQuery = new StringBuilder();
		String strPartitionKey;
		
		this.strUID = Long.toString(lUID);
		
		try{
			strPartitionKey = CollectorDBI.createDailyPartition(pstmtPartitionTables, lUID, null);
			
			sbQuery	.append(strProfilerQry.replaceAll("@KEY@", strPartitionKey))
					.append("(")
					// ?,getgmtnow(), ?,?,?::timestamp,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
					.append(strUID).append(", ")
					.append("getgmtnow(), ")
					.append(lThreadId).append(", ")
					.append(UtilsFactory.makeValidVarchar(strType)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strStartTime)).append("::timestamp, ")
					.append(nDurationInMilliSec).append(", ")
					.append(lApproxStartTimeInNanoSec).append(", ")
					.append(duration_ns).append(", ")
					.append(UtilsFactory.makeValidVarchar(strRefererURI)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strRequestURI)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strLocalHostnameIP)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strClassName)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strMethodName)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strMethodSignature)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strQuery)).append(", ")
					.append(lCallerMethodId).append(", ")
					.append(lCalleeMethodId).append(", ")
					.append(UtilsFactory.makeValidVarchar(strExceptionType)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strExceptionMessage)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strExceptionStackTrace)).append(")");
					
			// params:	lThreadId, strType, strStartTime, nDurationInMilliSec, lApproxStartTimeInNanoSec, duration_ns, strRequestURI, strClassName, strMethodName, strMethodSignature, strQuery
			stmtJavaProfiler.addBatch(sbQuery.toString());
			
			nBatchCount++;
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
		
		UtilsFactory.clearCollectionHieracy(sbQuery);
		sbQuery = null;
		
		strPartitionKey = null;
	}
	
	/**
	 * Insert the batched profiler entries in the table given in the initializeInsertBatch()
	 * 
	 * @throws Exception
	 */
	public void executeCounterBatch(long lDistributorThreadId) throws Exception {
//		Iterator<String> iterKeys = null;
//		String strPartitionKey = null;
		int ins[], nInserted = 0;
		
		try{
//			iterKeys = hmStmtJavaProfiler.keySet().iterator();
//			
//			while( iterKeys.hasNext() ){
//				strPartitionKey = iterKeys.next();
//				stmtJavaProfiler = hmStmtJavaProfiler.get(strPartitionKey);
//				
//				synchronized ( stmtJavaProfiler ) {
					// execute the batch for the looping UID_YEAR
					ins = stmtJavaProfiler.executeBatch();
					
					// keep track of inserted count
					nInserted += ins.length;
//				}
//			}
			
			LogManager.logDBInserts("JavaProfiler thread "+lDistributorThreadId+" inserted: "+nInserted+" <> UID: "+strUID);
			
			/*
			for(int i=0; i<ins.length; i++)	System.out.print(ins[i]+", ");
			System.out.println();
			*/
			nBatchCount = 0;
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			UtilsFactory.printSQLNextExceptions("executeCounterBatch", ex);
			throw ex;
		} finally {
			ins = null;
			nInserted = 0;
		}
	}
	
	/**
	 * Returns the batch size.
	 * 
	 * @return
	 */
	public int getBatchCount() {
		return nBatchCount;
	}
	
	/**
	 * Returns the last threadId used, for the given GUID's tomcat.
	 * If no profiler entry is found then returns 0.
	 * 
	 * @param con
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public long getLastThreadId(Connection con, long lUID) throws Exception {
		long lLastThreadId = 0l;
		
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			String strQuery = "SELECT COALESCE(max(thread_Id),0) AS lastThreadId FROM tomcat_profiler_"+Long.toString(lUID);
			
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			while( rst.next() ){
				lLastThreadId = rst.getLong("lastThreadId");
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		return lLastThreadId;
	}
	
	/**
	 * Clear all the Prepared Statement created
	 */
	public void clearPreparedStatement() {
		DataBaseManager.close(stmtJavaProfiler);
		stmtJavaProfiler = null;
		
		DataBaseManager.close(pstmtPartitionTables);
		pstmtPartitionTables = null;
	}
	
	/*
	@Override
	protected void finalize() throws Throwable {
		clearAllPreparedStatements();
		
		UtilsFactory.clearCollectionHieracy(hmPreStmtJavaProfiler);
		
		DataBaseManager.close(con);
		con = null;
		
		super.finalize();
	}
	*/
}
