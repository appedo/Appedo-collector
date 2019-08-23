package com.appedo.webcollector.webserver.DBI;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONObject;

public class NetStackDBI {

	public CallableStatement createNetStackTablePreparedStmt(Connection con) throws Throwable {
		CallableStatement callableStmt = null;
		StringBuilder sbQuery = new StringBuilder();
		try {
			sbQuery.append("{ call insert_net_stack_data(?, ?, ?, ?, ?::json)}");
			callableStmt = con.prepareCall(sbQuery.toString());
		} catch (Throwable t) {
			LogManager.errorLog("Exception while preparing connection statement for NetStack data batch ");
			LogManager.errorLog(t, sbQuery);
			throw t;
		} finally {
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
		return callableStmt;
	}
	
	public void addNetStackDataToBatch(CallableStatement callableStmt, JSONObject joNetStackData, long lUID) throws Throwable {
		
		try {
			String strCounterDataType = joNetStackData.getString("type");
			//long jaSize = JSONArray.fromObject(joNetStackData.getString(strCounterDataType)).size();
			callableStmt.setLong(1, lUID);
			callableStmt.setString(2, joNetStackData.getString("guid"));
			callableStmt.setString(3, joNetStackData.getString("datetime"));
			callableStmt.setString(4, strCounterDataType);
			callableStmt.setString(5, joNetStackData.getString(strCounterDataType).toString());
			//Add to Batch
			callableStmt.addBatch();
		} catch (Throwable t) {
			LogManager.errorLog("Exception while processing: "+joNetStackData.toString());
			LogManager.errorLog(t);
			throw t;
		} 
	}
	
	public int insertNetStackDataBatch(CallableStatement callableStmt) throws Throwable {
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
}
