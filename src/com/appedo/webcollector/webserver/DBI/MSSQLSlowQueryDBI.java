package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * MSSQL Slow Query related Database Interface layer.
 * This do the operations related to the MSSQL Slow Query records table.
 * 
 * @author Ramkumar
 *
 */
public class MSSQLSlowQueryDBI {
	
	private String strMSSQLSlowQueriesQuery = null;
	
//	private PreparedStatement psAgentResponse = null;
//	private PreparedStatement psDotNetProfiler = null;
	
	//private HashMap<String, PreparedStatement> hmPreStmtDotNetProfiler =  new HashMap<String, PreparedStatement>();
	
	private Statement stmtMSSQLSlowQuery = null;
	private PreparedStatement pstmtPartitionTables = null;
	
//	private int nBatchCount = 0;
	
//	private String strUID = "";
	
	/**
	 * Initializes the prepared statements with the respective insert queries.
	 * The prepared statements will be then used in the queuing and execution process
	 * 
	 * @param con
	 * @throws Exception
	 */
	public void initializeInsertBatch(Connection con) throws Exception {
		
		try{
			strMSSQLSlowQueriesQuery = "INSERT INTO MSSQL_slowquery_@KEY@ (uid,appedo_received_on, query, calls, duration_ms) VALUES ";
			
			stmtMSSQLSlowQuery = con.createStatement();
			
			pstmtPartitionTables = con.prepareStatement("SELECT create_asd_daily_partition_tables(?, ?, false, true, false, false)");
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
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
	 * @param strStartTime
	 * @param nDurationInMilliSec
	 * @param strClassName
	 * @param strMethodName
	 * @param strMethodSignature
	 * @param lCallerMethodId
	 * @param lCalleeMethodId
	 * @throws Exception
	 *
	public void addCounterBatch(Connection con, long lUID, String strQuery, long lCalls, long lDuration_ms) throws Exception {
		StringBuilder sbQuery = new StringBuilder();
		String strPartitionKey;
		
		this.strUID = Long.toString(lUID);
		strPartitionKey = strUID+"_"+UtilsFactory.getYYYYMMDD();
		
		try{
			sbQuery.append(strMSSQLSlowQueriesQuery.replaceAll("@KEY@", strPartitionKey))
					.append("(")
					.append(lUID).append(", ")
					.append("getgmtnow(), ").append("'")
					.append(strQuery).append("'").append(" , ")
					.append(lCalls).append(", ")
					.append(lDuration_ms).append(")");
					
			// params:	lThreadId, strType, strStartTime, nDurationInMilliSec, lApproxStartTimeInNanoSec, duration_ns, strRequestURI, strClassName, strMethodName, strMethodSignature, strQuery
			
			stmtMSSQLSlowQuery.addBatch(sbQuery.toString());
			
			nBatchCount++;
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
		
		UtilsFactory.clearCollectionHieracy(sbQuery);
		sbQuery = null;
		
		strPartitionKey = null;
	}*/
	
	/**
	 * Insert the batched profiler entries in the table given in the initializeInsertBatch()
	 * 
	 * @throws Exception
	 *
	public void executeCounterBatch(long lDistributorThreadId) throws Exception {
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
					ins = stmtMSSQLSlowQuery.executeBatch();
					
					// keep track of inserted count
					nInserted += ins.length;
//				}
				
				LogManager.logDBInserts("MSSQL Slow Query thread "+lDistributorThreadId+" inserted: "+nInserted+" <> UID: "+strUID);
				
				/*
				for(int i=0; i<ins.length; i++)	System.out.print(ins[i]+", ");
				System.out.println();
				*
				
				nBatchCount = 0;
			} catch(Exception ex) {
				LogManager.errorLog(ex);
				throw ex;
			} finally {
				ins = null;
				nInserted = 0;
			}
//		}
		
//		strPartitionKey = null;
//		iterKeys = null;
	}*/
	
	/**
	 * Returns the batch size.
	 * 
	 * @return
	 *
	public int getBatchCount() {
		return nBatchCount;
	}*/
	
	/**
	 * Execute the data INSERT into the Slow Procedure's partition table.
	 * 
	 * @param con
	 * @param lUID
	 * @param strQuery
	 * @param lCalls
	 * @param lDuration_ms
	 * @throws Exception
	 */
	public void execute(Connection con, long lUID, String strQuery, long lCalls, long lDuration_ms) throws Exception {
		StringBuilder sbQuery = new StringBuilder();
		String strPartitionKey;
		
		try{
			strPartitionKey = CollectorDBI.createDailyPartition(pstmtPartitionTables, lUID, null);
			
			sbQuery	.append(strMSSQLSlowQueriesQuery.replaceAll("@KEY@", strPartitionKey))
					.append("(")
					.append(lUID).append(", ")
					.append("getgmtnow(), ").append("'")
					.append(strQuery).append("'").append(" , ")
					.append(lCalls).append(", ")
					.append(lDuration_ms).append(")");
			
			stmtMSSQLSlowQuery.executeUpdate(sbQuery.toString());
			
		} catch(Exception ex) {
			if ( ! ex.getLocalizedMessage().contains("duplicate key value violates unique constraint") ) {
				LogManager.errorLog(ex);
				throw ex;
			}
		} finally {
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
	}
	
	/**
	 * Clear the Prepared Statement created
	 */
	public void clearPreparedStatement() {
		DataBaseManager.close(stmtMSSQLSlowQuery);
		stmtMSSQLSlowQuery = null;
		
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
