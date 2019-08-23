package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * .Net Profiler related Database Interface layer.
 * This do the operations related to the .Net Profiler records table.
 * 
 * @author Ramkumar
 *
 */
public class DotNetProfilerDBI {
	
	private String strProfilerQry = null;
	
//	private PreparedStatement psAgentResponse = null;
//	private PreparedStatement psDotNetProfiler = null;
	
	//private HashMap<String, PreparedStatement> hmPreStmtDotNetProfiler = new HashMap<String, PreparedStatement>();
	
	private Statement stmtDotNetProfiler = null;
	private PreparedStatement pstmtPartitionTables = null;
	
	private int nBatchCount = 0;
	
	private String strUID = "";
	
	/**
	 * Insert the given method traces into DB. And then insert a Queue entry.
	 * Insert happens in Stream format. So it should be so fast.
	 * 
	 * @param con
	 * @param strMethodsCSV
	 * @return
	 */
	public long insertMethodCalls(Connection con, String strGUID, long lUID, String strMethodsCSV) {
		long lInserts = 0;
		
		Statement stmt = null;
		
		Date dateLog = LogManager.logMethodStart();
		
		try{
			stmt = con.createStatement();
			
			//stmt.execute("CREATE TEMP TABLE dotnet_profiler_method_trace_"+lUID+"_temp (LIKE dotnet_profiler_method_trace_"+lUID+" );");
			//stmt.execute("ALTER TABLE dotnet_profiler_method_trace_"+lUID+"_temp DROP COLUMN id;");
			
			lInserts = DataBaseManager.doBulkInsert(con, "dotnet_profiler_method_trace_"+lUID+"(method_trace)", strMethodsCSV);

			//stmt.execute("INSERT INTO dotnet_profiler_method_trace_"+lUID+"(method_trace) SELECT method_trace FROM dotnet_profiler_method_trace_"+lUID+"_temp ");
			
			//stmt.execute("DROP TABLE dotnet_profiler_method_trace_"+lUID+"_temp");
			
			// if more than 1 row added then, log an entry in the UnComputed List.
			if( lInserts > 0 ) {
				stmt.executeUpdate("INSERT INTO dotnet_profiler_uncomputed_guids (guid, uid, rows_inserted, logged_on) VALUES ('"+strGUID+"',"+lUID+", "+lInserts+", now());");
			}
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		LogManager.logMethodEnd(dateLog);
		return lInserts;
	}
	
	/**
	 * Get the next Computable GUID.
	 * One GUID can send any number of request, but respective GUID stack-trace should be processed by only on Thread.
	 * So this procedure will give one GUID, after locking it.
	 * So that, no other Thread can use it, until the GUID is unlocked.
	 * 
	 * @param con
	 * @return
	 */
	public Object[] getNextComputeTraceDetails(Connection con) {
		strUID = null;
		Long lSerialId = null;
		
		Statement stmt = null;
		ResultSet rst = null;

		Date dateLog = LogManager.logMethodStart();
		
		try{
			stmt = con.createStatement();
			rst = stmt.executeQuery("SELECT * FROM get_next_dotnet_compute_guid()");
			
			if( rst.next() ) {
				strUID = rst.getString("uid");
				lSerialId = rst.getLong("id");
			}
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		LogManager.logMethodEnd(dateLog);
		
		return new Object[]{strUID, lSerialId};
	}
	
	/**
	 * Move the computed GUID summary entry into history table.
	 * 
	 * @param con
	 * @param lUnComputeSerialId
	 */
	public void removeSummaryEntry(Connection con, long lUnComputeSerialId) {
		Statement stmt = null;
		ResultSet rst = null;
		
		Date dateLog = LogManager.logMethodStart();
		
		try{
			stmt = con.createStatement();
			rst = stmt.executeQuery("SELECT remove_dotnet_computed_guid("+lUnComputeSerialId+")");
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Get the first <MAX_PROFILER_METHOD_TRACE_TO_COMPUTE> rows from the method_trace of the given UID table.
	 * 
	 * @param con
	 * @param strUID
	 * @return
	 */
	public ArrayList<String> getMethodTraceDetails(Connection con, String strUID) {
		ArrayList<String> alMethodTraces = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		int nIndex = 0;
		long lMinSerialId = -2, lMaxSerialId = -1;
		
		Date dateLog = LogManager.logMethodStart();
		
		try{
			stmt = con.createStatement();
			rst = stmt.executeQuery("SELECT id, method_trace FROM dotnet_profiler_method_trace_"+strUID+" ORDER BY id LIMIT "+Constants.MAX_PROFILER_METHOD_TRACE_TO_COMPUTE);
			
			alMethodTraces = new ArrayList<String>();
			
			while( rst.next() ) {
				// Get the min serial-id, which is the first value
				if( nIndex == 0 ) {
					lMinSerialId = rst.getLong("id");
				}
				alMethodTraces.add(rst.getString("method_trace"));
				
				lMaxSerialId = rst.getLong("id");
				
				nIndex++;
			}
			
			// Get the max serial_id, which is the last value
			if( nIndex != 0 ) {
				stmt.execute("INSERT INTO dotnet_profiler_method_trace_"+strUID+"_history SELECT * FROM dotnet_profiler_method_trace_"+strUID+" WHERE id BETWEEN "+lMinSerialId+" AND "+lMaxSerialId);
				stmt.execute("DELETE FROM dotnet_profiler_method_trace_"+strUID+" WHERE id BETWEEN "+lMinSerialId+" AND "+lMaxSerialId);
			}
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		LogManager.logMethodEnd(dateLog);
		
		return alMethodTraces;
	}
	
	/**
	 * Get the last Transaction-Id (Thread-Id) used for the GUID.
	 * If the transaction table is blank then, need to assign 1.
	 * 
	 * @param con
	 * @param strUID
	 * @param strThreadId
	 * @return
	 */
	public Long getLastUniqueThreadIdAssigned(Connection con, String strUID, String strThreadId) {
		Statement stmt = null;
		ResultSet rst = null;
		StringBuilder sbQuery = new StringBuilder();
		
		Long lLastUniqueThreadIdAssigned = null;
		
		Date dateLog = LogManager.logMethodStart();
		
		try{
			sbQuery.append("SELECT max(thread_id) AS thread_id FROM dotnet_profiler_"+strUID);
			
			stmt = con.createStatement();
			rst = stmt.executeQuery(sbQuery.toString());
			
			while( rst.next() ) {
				lLastUniqueThreadIdAssigned = rst.getLong("thread_id");
			}
			
		} catch(Exception ex) {
			LogManager.errorLog(ex, sbQuery);
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		LogManager.logMethodEnd(dateLog);
		
		return lLastUniqueThreadIdAssigned;
	}
	
	/**
	 * Add/Update the Method's Name-Ids, for the given UID.
	 * This comes as "FE,...." from the agent. 
	 * 
	 * @param con
	 * @param strUID
	 * @param strMethodId
	 * @param strMethodName
	 * @throws Exception
	 */
	public void addMethodReference(Connection con, String strUID, String strMethodId, String strMethodName) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		StringBuilder sbQuery = new StringBuilder();
		Statement stmt = null;
		
		try{
			sbQuery	.append("SELECT add_profiler_method_reference(").append(strUID).append(",").append(strMethodId).append(",'").append(strMethodName).append("')");
			
			stmt = con.createStatement();
			stmt.execute(sbQuery.toString());
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
			
			
			LogManager.logMethodEnd(dateLog);
		}
	}
	
	/**
	 * Get all the Method Name-Id pairs from the DB for the given UID
	 * 
	 * @param con
	 * @param strUID
	 * @return
	 * @throws Exception
	 */
	public Hashtable<String, String> getMethodReferences(Connection con, String strUID) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		StringBuilder sbQuery = new StringBuilder();
		Statement stmt = null;
		ResultSet rst = null;
		
		Hashtable<String, String> htMethodNameIdPair = null;
		
		try{
			sbQuery	.append("SELECT method_id, method_name FROM dotnet_profiler_method_reference_").append(strUID);
			
			stmt = con.createStatement();
			rst = stmt.executeQuery(sbQuery.toString());
			
			htMethodNameIdPair = new Hashtable<String, String>();
			
			while( rst.next() ) {
				htMethodNameIdPair.put(rst.getString("method_id"), rst.getString("method_name"));
			}
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
			
			
			LogManager.logMethodEnd(dateLog);
		}
		
		return htMethodNameIdPair;
	}
	
	/**
	 * Clear the given Method's Name-Ids, for the given UID.
	 * This comes as "FL,...." from the agent.
	 * 
	 * @param con
	 * @param strUID
	 * @param strFunctionId
	 * @throws Exception
	 */
	public void deleteMethodReference(Connection con, String strUID, String strFunctionId) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		StringBuilder sbQuery = new StringBuilder();
		Statement stmt = null;
		
		try{
			sbQuery	.append("DELETE FROM dotnet_profiler_method_reference_").append(strUID).append(" WHERE method_id = ").append(strFunctionId);
			
			stmt = con.createStatement();
			stmt.executeUpdate(sbQuery.toString());
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
			
			
			LogManager.logMethodEnd(dateLog);
		}
	}
	
	/**
	 * Clear all the Method's Name-Ids, for the given UID.
	 * This comes as "S,...." from the agent. 
	 * 
	 * @param con
	 * @param strUID
	 * @throws Exception
	 */
	public void clearMethodReferences(Connection con, String strUID) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		StringBuilder sbQuery = new StringBuilder();
		Statement stmt = null;
		
		try{
			sbQuery	.append("TRUNCATE dotnet_profiler_method_reference_").append(strUID).append(" RESTART IDENTITY");
			
			stmt = con.createStatement();
			stmt.executeUpdate(sbQuery.toString());
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
			
			
			LogManager.logMethodEnd(dateLog);
		}
	}
	
	/**
	 * Initializes the prepared statements with the respective insert queries.
	 * The prepared statements will be then used in the queuing and execution process
	 * 
	 * @param con
	 * @throws Exception
	 */
	public void initializeInsertBatch(Connection con) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		try{
			strProfilerQry = "INSERT INTO dotnet_profiler_@KEY@ (uid, appedo_received_on, thread_id, start_time, duration_ms, request_uri, class_name, method_name, method_signature, caller_method_id, callee_method_id) VALUES ";
			
			stmtDotNetProfiler = con.createStatement();
			
			pstmtPartitionTables = con.prepareStatement("SELECT create_asd_daily_partition_tables(?, ?, false, false, false, true)");
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			LogManager.logMethodEnd(dateLog);
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
	 * @param lStartTimeInMilliSec
	 * @param lDurationInMilliSec
	 * @param strClassName
	 * @param strMethodName
	 * @param strMethodSignature
	 * @param lCallerMethodId
	 * @param lCalleeMethodId
	 * @throws Exception
	 */
	public void addCounterBatch(Connection con, String strUID, long lThreadId, long lStartTimeInMilliSec, long lDurationInMilliSec, String strRequestURI, String strClassName, String strMethodName, String strMethodSignature, long lCallerMethodId, long lCalleeMethodId) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		StringBuilder sbQuery = new StringBuilder();
		String strPartitionKey;
		
		this.strUID = strUID;
		
		try{
			strPartitionKey = CollectorDBI.createDailyPartition(pstmtPartitionTables, Long.parseLong(strUID), null);
			
			sbQuery.append(strProfilerQry.replaceAll("@KEY@", strPartitionKey))
					.append("(")
					// uid, appedo_received_on, thread_id, start_time, duration_ms, request_uri, class_name, method_name, method_signature, caller_method_id, callee_method_id
					// (?,getgmtnow(),?,?::timestamp,?,?,?,?,?,?,?)
					.append(strUID).append(", ")
					.append("getgmtnow(), ")
					.append(lThreadId).append(", ")
					.append("to_timestamp(").append(lStartTimeInMilliSec).append("::float/1000), ")
					.append(lDurationInMilliSec).append(", ")
					.append(UtilsFactory.makeValidVarchar(strRequestURI)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strClassName)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strMethodName)).append(", ")
					.append(UtilsFactory.makeValidVarchar(strMethodSignature)).append(", ")
					.append(lCallerMethodId).append(", ")
					.append(lCalleeMethodId).append(")");
					
			// params:	lThreadId, strType, strStartTime, nDurationInMilliSec, lApproxStartTimeInNanoSec, duration_ns, strRequestURI, strClassName, strMethodName, strMethodSignature, strQuery
			
			stmtDotNetProfiler.addBatch(sbQuery.toString());
			
			nBatchCount++;
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
		
		UtilsFactory.clearCollectionHieracy(sbQuery);
		sbQuery = null;
		
		strPartitionKey = null;

		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Insert the batched profiler entries in the table given in the initializeInsertBatch()
	 * 
	 * @throws Exception
	 */
	public void executeCounterBatch(long lDistributorThreadId) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
//		Iterator<String> iterKeys = null;
//		PreparedStatement psDotNetProfiler = null;
//		String strPartitionKey = null;
		int ins[], nInserted = 0;
			
			try{
//				iterKeys = hmPreStmtDotNetProfiler.keySet().iterator();
//				
//				while( iterKeys.hasNext() ){
//					strPartitionKey = iterKeys.next();
//					psDotNetProfiler = hmPreStmtDotNetProfiler.get(strPartitionKey);
					
					// execute the batch for the looping UID_YEAR
					ins = stmtDotNetProfiler.executeBatch();
					
					// keep track of inserted count
					nInserted += ins.length;
//				}
				
				//System.out.println(".NetProfiler thread "+lDistributorThreadId+" inserted: "+nInserted);
				LogManager.logDBInserts("DNP <> .NetProfiler thread "+lDistributorThreadId+" inserted: "+nInserted+" <> UID: "+strUID);
				
				/*
				for(int i=0; i<ins.length; i++)	System.out.print(ins[i]+", ");
				System.out.println();
				*/
				
				nBatchCount = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
				throw ex;
			} finally {
				ins = null;
				nInserted = 0;
			}
			
			LogManager.logMethodEnd(dateLog);
			
//		}
		
//		strPartitionKey = null;
//		iterKeys = null;
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
	 * Clear the Prepared Statement created
	 */
	public void clearPreparedStatement() {
		DataBaseManager.close(stmtDotNetProfiler);
		stmtDotNetProfiler = null;
		
		DataBaseManager.close(pstmtPartitionTables);
		pstmtPartitionTables = null;
	}
	
	/*
	@Override
	protected void finalize() throws Throwable {
		clearAllPreparedStatements();
		
		UtilsFactory.clearCollectionHieracy(hmPreStmtDotNetProfiler);
		
		strProfilerQry = null;
		
		super.finalize();
	}
	*/
}
