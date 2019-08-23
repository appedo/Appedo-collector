package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;

public class BeatsDBI {

	public void execute(Connection con, String strCounterEntry) {
		PreparedStatement preStmt = null;
		try{
			preStmt = con.prepareStatement("update elk_client_details set agent_status_time = now() where guid = ?");
			preStmt.setString(1,strCounterEntry);
			preStmt.executeUpdate();
		}catch(Exception e){
			LogManager.errorLog(e);
		}finally{
			DataBaseManager.close(preStmt);
			preStmt = null;
		}
		
	}
}
