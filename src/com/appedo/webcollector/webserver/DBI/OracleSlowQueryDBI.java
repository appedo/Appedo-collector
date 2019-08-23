package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * OracleSlowQueries related Database Interface layer.
 * This do the operations related to the OracleSlowQueries records table.
 * 
 * @author Ramkumar
 *
 */
public class OracleSlowQueryDBI {
	
	private String strOracleSlowQueriesQry = null;
	
//	private PreparedStatement psAgentResponse = null;
//	private PreparedStatement psPGSlowQueries = null;
	
	//private HashMap<String, PreparedStatement> hmPreStmtPGSlowQueries =  new HashMap<String, PreparedStatement>();
	
	private Statement stmtOracleSlowQueries = null;
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
			strOracleSlowQueriesQry = "INSERT INTO oracle_slowquery_@KEY@ (uid, appedo_received_on, Query, calls, duration_ms) VALUES ";
			
			stmtOracleSlowQueries = con.createStatement();
			
			pstmtPartitionTables = con.prepareStatement("SELECT create_asd_daily_partition_tables(?, ?, false, true, false, false)");
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
	}
	
	/**
	 * Insert the one-time response entry for each PGSlowQueries-set when received.
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
	 * Queue the PGSlowQueries entry into the batch. The batch will be executed when the executeCounterBatch is called.
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
		strPartitionKey = this.strUID+"_"+UtilsFactory.getYYYYMMDD();
		
		try{
			sbQuery.append(strOracleSlowQueriesQry.replaceAll("@KEY@", strPartitionKey))
					.append("(")
					.append(lUID).append(", ")
					.append("getgmtnow(), ").append("'")
					.append(strQuery).append("'").append(" , ")
					.append(lCalls).append(", ")
					.append(lDuration_ms).append(")");
					
			// params:	lThreadId, strType, strStartTime, nDurationInMilliSec, lApproxStartTimeInNanoSec, duration_ns, strRequestURI, strClassName, strMethodName, strMethodSignature, strQuery
			
			stmtOracleSlowQueries.addBatch(sbQuery.toString());
			
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
	 * Insert the batched PGSlowQueries entries in the table given in the initializeInsertBatch()
	 * 
	 * @throws Exception
	 *
	public void executeCounterBatch(long lDistributorThreadId) throws Exception {
//		Iterator<String> iterKeys = null;
//		PreparedStatement psPGSlowQueries = null;
//		String strPartitionKey = null;
		int ins[], nInserted = 0;
			
			try{
//				iterKeys = hmPreStmtPGSlowQueries.keySet().iterator();
//				
//				while( iterKeys.hasNext() ){
//					strPartitionKey = iterKeys.next();
//					psPGSlowQueries = hmPreStmtPGSlowQueries.get(strPartitionKey);
					
					// execute the batch for the looping UID_YEAR
					ins = stmtOracleSlowQueries.executeBatch();
					
					// keep track of inserted count
					nInserted += ins.length;
//				}
				
				LogManager.logDBInserts("PGSlowQueries thread "+lDistributorThreadId+" inserted: "+nInserted+" <> UID: "+strUID);
				
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
	 * Execute the data INSERT into the SlowQueries's partition table.
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
			
			sbQuery.append(strOracleSlowQueriesQry.replaceAll("@KEY@", strPartitionKey))
					.append("(").append(lUID).append(", ")
					.append("getgmtnow(), ")
					.append("'").append(strQuery).append("'").append(", ")
					.append(lCalls).append(", ")
					.append(lDuration_ms)
					.append(")");
			
			stmtOracleSlowQueries.executeUpdate(sbQuery.toString());
			
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
		DataBaseManager.close(stmtOracleSlowQueries);
		stmtOracleSlowQueries = null;
		
		DataBaseManager.close(pstmtPartitionTables);
		pstmtPartitionTables = null;
	}
	
	/*
	@Override
	protected void finalize() throws Throwable {
		clearAllPreparedStatements();
		
		UtilsFactory.clearCollectionHieracy(hmPreStmtPGSlowQueries);
		
		strPGSlowQueriesQry = null;
		
		super.finalize();
	}
	*/
}
