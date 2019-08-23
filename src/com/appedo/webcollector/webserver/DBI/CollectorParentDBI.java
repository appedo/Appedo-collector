package com.appedo.webcollector.webserver.DBI;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.NotificationBean;
import com.appedo.webcollector.webserver.bean.UnifiedCounterDataBean;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * Abstract class which should be extended by all module DBI classes.
 * This class has the basic operations like insertion into response table & counter table as batch.
 * 
 * Child class must define initalizeInsertBatch(), with the Insert query for target table.
 * 
 * @author Ramkumar R
 * 
 */
public class CollectorParentDBI {
	
	protected String strPerformanceCounter = null;
	
//	protected HashMap<String, Statement> hmStmtPerformanceCounter = new HashMap<String, Statement>();
	
	protected Statement stmtPerformanceCounter = null;
	protected ArrayList<String> alQueries = null;
	
	protected PreparedStatement pstmtPartitionTables = null;
	
	protected Statement stmtAgentResponseInMin = null;
	protected Statement stmtPerformanceCounterInMin = null;
	
	private String strUID = "";
	
	/**
	 * Constructor which gets the db connection object to be used for the counters insert operations.
	 * What kind of agent is also defined in this.
	 * 
	 * @param con
	 * @param nIndex
	 *
	public CollectorParentDBI(int nIndex) {
		this.nIndex = nIndex;
	}*/
	
	/** ## This CollectorParentDBI was used as Parent for ApplicationPerformanceCounterDBI , ServerPerformanceCounterDBI etc ##
	 *  ## this method was overridden there; now added in this Class itself ##
	 *  
	 * Child class must define initalizeInsertBatch(), with the Insert query for target table.
	 * 
	 * @throws Exception
	 *
	public abstract void initializeInsertBatch() throws Exception;
	*/
	
	/**
	 * Initializes the prepared statement which has insert queries for the response table and the counter table.
	 * 
	 * @throws Exception
	 */
	public void initializeInsertBatch(Connection con) throws Exception {
		
		try{
			// For each counter in the received agent response; with a reference for above entry.
			strPerformanceCounter = "INSERT INTO collector_@KEY@ (uid, received_on, appedo_received_on, agent_version, counter_type, counter_value, exception, top_process, is_first) VALUES ";
			
			stmtPerformanceCounter = con.createStatement();
			alQueries = new ArrayList<String>();
			
			pstmtPartitionTables = con.prepareStatement("SELECT create_asd_daily_partition_tables(?, ?, true, false, false, false)");
			
			/* TODO table partitioning
			// Agent Response once in a min
			psAgentResponseInMin = con.prepareStatement("INSERT INTO application_agent_response_1_min (uid, received_on, appedo_received_on, agent_version, exception) VALUES (?, ?, ?, ?, ?) ", Statement.RETURN_GENERATED_KEYS);
			
			// Counter insert - Average Counter value once in a min
			psPerformanceCounterInMin = con.prepareStatement("INSERT INTO application_performance_counter_1_min (agent_response_id, counter_type, counter_value, exception) VALUES (?,?,?,?) ");
			*/
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
	}
	
	/**
	 * Queues the given counter code-value pair into the table defined in the initalizeInsertBatch(). 
	 * The batch will be executed when executeCounterBatch() is called.
	 * 
	 * @param lAgentResponseId
	 * @param nCounterCode
	 * @param dCounterValue
	 * @throws Exception
	 */
	public void addCounterBatch(long lUID, Timestamp timestampAgent, Timestamp timestampAppedo, String strAgentVersion, int nCounterCode, Double dCounterValue, String strCounterError, String strTopProcValue, boolean is_first) throws Exception {
		StringBuilder sbQuery = new StringBuilder();
		String strInputDate, strPartitionKey;
		
		this.strUID = Long.toString(lUID);
		strInputDate = UtilsFactory.formatYYYYMMDD( timestampAppedo.getTime() );
		
		try{
			strPartitionKey = CollectorDBI.createDailyPartition(pstmtPartitionTables, lUID, strInputDate);
			
			sbQuery.append(strPerformanceCounter.replaceAll("@KEY@", strPartitionKey)).append("(")
					.append(strUID).append(", ")
					.append("to_timestamp( ").append(timestampAgent.getTime()).append("::float/1000 ), ")
					.append("to_timestamp( ").append(timestampAppedo.getTime()).append("::float/1000 ), ")
					.append(UtilsFactory.makeValidVarchar(strAgentVersion)).append(", ")
					.append(nCounterCode).append(", ")
					.append(dCounterValue).append(", ")
					.append(UtilsFactory.makeValidVarchar(strCounterError))
					.append(", '")
					.append(strTopProcValue).append("', ")
					.append(is_first)
					.append(")");

//			synchronized ( stmtPerformanceCounter ) {
				alQueries.add(lUID+" <> "+strInputDate+" <> "+strPartitionKey+" <> "+sbQuery.toString()+"\n");
				stmtPerformanceCounter.addBatch(sbQuery.toString());
//			}
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			SQLException sqlExcpNext = null;
			if( ex instanceof SQLException ) {
				while( (sqlExcpNext = ((SQLException)ex).getNextException()) != null) {
					LogManager.errorLog(sqlExcpNext);
					sqlExcpNext.printStackTrace();
				}
			}
			
			throw ex;
		} finally {
			strPartitionKey = null;
		}
	}
	
	public PreparedStatement initiateNotificationPreparedStmt(Connection con) throws Throwable {
		PreparedStatement pstmt = null;
		StringBuilder sbQuery = new StringBuilder();
		try {
			sbQuery.append("INSERT INTO notification(system_id, system_uuid, received_on, appedo_received_on, log_level, system_name, method, notification_message)")
					.append(" SELECT ?, system_uuid, now(), appedo_received_on, log_level, system_name, method, notification_message FROM ")
					.append(" json_populate_record(NULL::notification, ?::json) ");
			
			pstmt = con.prepareStatement(sbQuery.toString());
			
		} catch (Throwable t) {
			LogManager.errorLog("Exception while preparing connection statement for Unified Agent Collector Batch : ");
			LogManager.errorLog(t, sbQuery);
			throw t;
		} finally {
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
		return pstmt;
	}
	
	public void addNotificationDataBatch(PreparedStatement pstmt,NotificationBean notificationBean, long lSystemId) throws Throwable {
		
		try {
			pstmt.setLong(1, lSystemId);
			pstmt.setString(2, notificationBean.getNotificationData().getJSONObject("notificationData").toString().replace("'", "''"));
			
			//Add to Batch
			pstmt.addBatch();
			
		} catch (Throwable t) {
			LogManager.errorLog("Exception while processing: "+notificationBean.toString());
			LogManager.errorLog(t);
			throw t;
		}
	}
	
	public int insertNotificationDataBatch(PreparedStatement pstmt) throws Throwable {
		int ins[], nInserted = 0;
		
		try {
			ins = pstmt.executeBatch();
			//pstmt.
			nInserted = ins.length;
		} catch (Throwable t) {
			int i =0;
			SQLException sqlExcpNext = null;
			if( t instanceof SQLException ) {
				while( (sqlExcpNext = ((SQLException)t).getNextException()) != null) {
					LogManager.errorLog(sqlExcpNext.getMessage());
					sqlExcpNext.printStackTrace();
					//To break the exception print after 10 error
					i++;
					if( i > 10 ) {
						break;
					}
				}
			}
			
			throw t;
		}
		return nInserted;
	}
	
	public CallableStatement createCollectorTablePrepredstmt(Connection con) throws Throwable {
		CallableStatement callableStmt = null;
		StringBuilder sbQuery = new StringBuilder();
		try {
			sbQuery.append("{ call insert_counter_data(?, ?, ?, ?, ?, ?::json)}");
			callableStmt = con.prepareCall(sbQuery.toString());
		} catch (Throwable t) {
			LogManager.errorLog("Exception while preparing connection statement for Unified Agent Collector Batch : ");
			LogManager.errorLog(t, sbQuery);
			throw t;
		} finally {
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
		return callableStmt;
	}
	
	public void addCounterDataToBatch(CallableStatement callableStmt, UnifiedCounterDataBean uaCounterDataBean, long lUID) throws Throwable {
		
		//System.out.println("date: "+uaCounterDataBean.getCounterData().getString("datetime"));
		
		try {
			String strCounterDataType = uaCounterDataBean.getCounterData().getString("type");
			//long jaSize = JSONArray.fromObject(uaCounterDataBean.getCounterData().getString(strCounterDataType)).size();
			//System.out.println("Type: "+strCounterDataType+", size:"+jaSize);
			callableStmt.setLong(1, lUID);
			callableStmt.setString(2, uaCounterDataBean.getCounterData().getString("guid"));
			callableStmt.setString(3, uaCounterDataBean.getCounterData().getString("datetime"));
			callableStmt.setString(4, uaCounterDataBean.getCounterData().getString("mod_type"));
			callableStmt.setString(5, strCounterDataType);
			callableStmt.setString(6, uaCounterDataBean.getCounterData().getString(strCounterDataType).toString());
			//Add to Batch
			callableStmt.addBatch();
		} catch (Throwable t) {
			LogManager.errorLog("Exception while processing: "+uaCounterDataBean.toString());
			LogManager.errorLog(t);
			throw t;
		} /*finally {
			//DataBaseManager.close(pstmt);
			//pstmt = null;
			
			//UtilsFactory.clearCollectionHieracy( sbQuery );
		}*/
	}
	
	public int insertCounterDatasBatch(CallableStatement callableStmt) throws Throwable {
		int ins[], nInserted = 0;
		
		try {
			ins = callableStmt.executeBatch();
			//pstmt.
			nInserted = ins.length;
		} catch (Throwable t) {
			int i =0;
			SQLException sqlExcpNext = null;
			if( t instanceof SQLException ) {
				while( (sqlExcpNext = ((SQLException)t).getNextException()) != null) {
					LogManager.errorLog(sqlExcpNext.getMessage());
					sqlExcpNext.printStackTrace();
					//To break the exception print after 10 error
					i++;
					if( i > 10 ) {
						break;
					}
				}
			}
			
			throw t;
		}
		return nInserted;
	}
	
	/**
	 * Insert a row in the respective response table for a min (taking average) to represent a counter-set is received.
	 * 
	 * @param lUID
	 * @param timestampAgent
	 * @param timestampAppedo
	 * @param strAgentVersion
	 * @param strAgentError
	 * @return
	 * @throws Exception
	 *
	public long insertAgentResponseInMin(long lUID, Timestamp timestampAgent, Timestamp timestampAppedo, String strAgentVersion, String strAgentError) throws Exception {
		long lReturn = 0;
		
		try{
			psAgentResponseInMin.setLong(1, lUID);
			psAgentResponseInMin.setTimestamp(2, timestampAgent);
			psAgentResponseInMin.setTimestamp(3, timestampAppedo);
			psAgentResponseInMin.setString(4, strAgentVersion);
			psAgentResponseInMin.setString(5, strAgentError);
			
			lReturn = DataBaseManager.insertAndReturnKey(psAgentResponseInMin, "response_id");
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
		
		return lReturn;
	}
	
	/**
	 * 
	 * 
	 * @param lAgentResponseId
	 * @param nCounterCode
	 * @param dCounterValue
	 * @param strCounterError
	 * @throws Exception
	 *
	public void addCounterBatchInMin(long lAgentResponseId, int nCounterCode, Double dCounterValue, String strCounterError) throws Exception {
		
		try{
			// params:	agent_response_id, counter_type, counter_value
			psPerformanceCounterInMin.setLong(1, lAgentResponseId);
			psPerformanceCounterInMin.setInt(2, nCounterCode);
			psPerformanceCounterInMin.setDouble(3, dCounterValue);
			psPerformanceCounterInMin.setString(4, strCounterError);
			psPerformanceCounterInMin.addBatch();
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
	}
	
	/**
	 * Insert the counters available in the batch into the table defined in the initalizeInsertBatch().
	 * 
	 * @throws Exception
	 */
	public void executeCounterBatch(long lThisThreadId) throws Throwable {
//		Iterator<String> iterKeys = null;
//		Statement stmtPerformanceCounter = null;
//		String strPartitionKey = null;
		int ins[], nInserted = 0;
		
		try{
//			iterKeys = hmStmtPerformanceCounter.keySet().iterator();
//			
//			while( iterKeys.hasNext() ){
//				strPartitionKey = iterKeys.next();
//				stmtPerformanceCounter = hmStmtPerformanceCounter.get(strPartitionKey);
//				synchronized ( stmtPerformanceCounter ) {
					// execute the batch for the looping UID_YEAR
					ins = stmtPerformanceCounter.executeBatch();
					/* TODO remove the prepared statement through Thread
					// once the batch is inserted the PreparedStatement should be closed.
					DataBaseManager.close(psPerformanceCounter);
					
					// remove the PreparedStatement; if required it will be added in next queuing 
					hmPreStmtPerformanceCounter.remove(strPartitionKey);
					*/
					// keep track of inserted count
					nInserted += ins.length;
//				}
//			}
			if( nInserted > 0 ) {
				LogManager.logDBInserts("PerfCounter thread "+lThisThreadId+" inserted: "+nInserted+" <> UID: "+strUID);
			}
			
			/*
			 * Insert 1 minute aggregations
			 */
			// TODO enable_1min int insForMin[] = psPerformanceCounterInMin.executeBatch();
			
//			if( insForMin.length > 0 ) {
//				System.out.println(this_agent+"-PerfCounterInMin inserted: "+insForMin.length);
//			}
			/*
			for(int i=0; i<ins.length; i++)	System.out.print(ins[i]+", ");
			System.out.println();
			*/
		} catch(Throwable th) {
			SQLException sqlExcpNext = null;
			if( th instanceof SQLException ) {
				LogManager.errorLog("executeBatch for queries: "+alQueries.toString());
				
				sqlExcpNext = (SQLException)th;
				if( sqlExcpNext.getNextException() != null && sqlExcpNext.getMessage().contains("syntax error at or near \"-\"") ) {
					LogManager.errorLog(sqlExcpNext);
				} else {
					while( (sqlExcpNext = ((SQLException)th).getNextException()) != null) {
						//if( ! sqlExcpNext.getMessage().contains("collector_0_2014") ) {
							LogManager.errorLog(sqlExcpNext);
						//}
					}
				}
			}
			
			throw th;
		} finally {
			UtilsFactory.clearCollectionHieracy(alQueries);
			
			DataBaseManager.close(stmtPerformanceCounter);
			stmtPerformanceCounter = null;
			
			DataBaseManager.close(pstmtPartitionTables);
			pstmtPartitionTables = null;
			
//			strPartitionKey = null;
//			iterKeys = null;
			ins = null;
			nInserted = 0;
		}
	}
	
	/**
	 * update `module_master` for uid's last appedo received on 
	 * 
	 * @param con
	 * @param lUID
	 * @param tsAppedoReceivedOn
	 * @throws Exception
	 */
	public void updateModuleLastAppedoReceivedOn(Connection con, long lUID, long lAppedoReceivedOn) throws Exception {
		Statement stmt = null;
		
		StringBuilder sbQuery = new StringBuilder();
		
		try {
			sbQuery	.append("UPDATE module_master SET ")
					.append("  last_appedo_received_on = to_timestamp(").append(lAppedoReceivedOn).append("::float/1000) ")
					.append("WHERE uid = ").append(lUID);
			
			stmt = con.createStatement();
			stmt.executeUpdate(sbQuery.toString());
			
		} catch (Exception e) {
			throw e;
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
		}
	}
	
	public PreparedStatement initiateUpdateModuleLastReceivedUpdate(Connection con) throws Throwable {
		PreparedStatement pstmt = null;
		StringBuilder sbQuery = new StringBuilder();
		try {
			sbQuery.append("UPDATE module_master set last_appedo_received_on = ?::timestamptz WHERE uid = ? ");
			pstmt = con.prepareStatement(sbQuery.toString());
		} catch (Throwable t) {
			LogManager.errorLog("Exception while preparing connection statement for Last appedo received on batch.");
			LogManager.errorLog(t, sbQuery);
			throw t;
		} finally {
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
		return pstmt;
	}
	
	public void addLastReceivedOnDataToBatch(PreparedStatement pstmt, Long lUID, String receivedOnDate) throws Throwable {
		StringBuilder sbQuery = new StringBuilder();
		
		try {
			pstmt.setString(1, receivedOnDate);
			pstmt.setLong(2, lUID);
			pstmt.addBatch();
		} catch (Throwable t) {
			LogManager.errorLog("Exception while processing this uid "+lUID);
			LogManager.errorLog(t, sbQuery);
			throw t;
		}
	}
	
	public int insertLastReceivedBatch(PreparedStatement pstmt) throws Throwable {
		int ins[], nInserted = 0;
		
		try {
			ins = pstmt.executeBatch();
			nInserted = ins.length;
		} catch (Throwable t) {
			SQLException sqlExcpNext = null;
			if( t instanceof SQLException ) {
				while( (sqlExcpNext = ((SQLException)t).getNextException()) != null) {
					LogManager.errorLog(sqlExcpNext.getMessage());
					sqlExcpNext.printStackTrace();
				}
			}
			
			throw t;
		}
		return nInserted;
	}
	
	public void updateBreachedUsers(Connection con, String userIds) throws Exception {
		Statement stmt = null;
		
		StringBuilder sbQuery = new StringBuilder();
		
		try {
			sbQuery	.append(" UPDATE user_pvt_counters ")
					.append(" SET oad_breached = TRUE ")
					.append(" WHERE user_id IN ").append(userIds);
			
			stmt = con.createStatement();
			stmt.executeUpdate(sbQuery.toString());
			
		} catch (Exception e) {
			throw e;
		} finally {
			DataBaseManager.close(stmt);
			stmt = null;
		}
	}
	
	/**
	 * Clear all the Prepared Statement created
	 *
	public void clearAllPreparedStatements() {
		
		try {
			Iterator<String> iterPreStmt = hmStmtPerformanceCounter.keySet().iterator();
			
			while( iterPreStmt.hasNext() ) {
				String strUID = iterPreStmt.next();
				DataBaseManager.close( hmStmtPerformanceCounter.get(strUID) );
				hmStmtPerformanceCounter.remove(strUID);
			}
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
	}
	*/
	
	/*
	@Override
	protected void finalize() throws Throwable {
		
		clearAllPreparedStatements();
		
		DataBaseManager.close(psAgentResponseInMin);
		psAgentResponseInMin = null;
		
		DataBaseManager.close(psPerformanceCounterInMin);
		psPerformanceCounterInMin = null;
		
		DataBaseManager.close(con);
		con = null;
		
		UtilsFactory.clearCollectionHieracy(hmPreStmtPerformanceCounter);
		
		super.finalize();
	}
	*/
	
	/*public static void main(String args[]) throws Exception {
		Connection con = null;
		
		
		
		con = DataBaseManager.giveConnection();
		
		long uid=1449;
		
		updateModuleLastAppedoReceivedOnTest(con, uid);
		
		DataBaseManager.close(con);
		
		
	}
	
	public static void updateModuleLastAppedoReceivedOnTest(Connection con, long lUID) throws Exception {
		PreparedStatement pstmt = null;
		
		StringBuilder sbQuery = new StringBuilder();
		
		try {
			sbQuery	.append("UPDATE module_master SET ")
					.append("  last_appedo_received_on = ?::timestamptz ")
					.append("WHERE uid = ").append(lUID);
			
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setString(1, "2017-06-07T09:57:53.077+05:30");
			
			pstmt.executeUpdate();
			//pstmt.executeUpdate(sbQuery.toString());
			
		} catch (Exception e) {
			throw e;
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
		}
	}*/
	
	public static void main(String[] args) {
		Throwable th = new org.postgresql.util.PSQLException("", null);
		System.out.println( th instanceof Throwable );
		System.out.println( th instanceof Exception );
		System.out.println( th instanceof SQLException );
		System.out.println( th instanceof SQLClientInfoException);
	}
}
