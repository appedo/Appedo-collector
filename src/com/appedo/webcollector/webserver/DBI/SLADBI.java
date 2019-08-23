package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.webcollector.webserver.bean.SLABean;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * DataBase Interface class which does the operations related to SLA breach
 * 
 * @author Ramkumar R
 *
 */
public class SLADBI {
	
	public ArrayList<SLABean> getAllSLAs(Connection con) throws Exception{
		Statement stmt = null;
		ResultSet rst = null;
		
		StringBuilder sbQuery = new StringBuilder();
		
		ArrayList<SLABean> alSLAs = new ArrayList<SLABean>();
		SLABean slaBean = null;
		
		try {
			sbQuery.append("SELECT sla.*, server_id, encrypted_server_id, server_name FROM sla INNER JOIN servermaster sm ON sm.server_id = reference_id AND module_type = 'SERVER'");
			
			stmt = con.createStatement();
			rst = stmt.executeQuery(sbQuery.toString());
			
			while (rst.next()) {
				slaBean = new SLABean();
				
				slaBean.setSLAId(rst.getLong("sla_id"));
				slaBean.setModuleType(rst.getString("module_type"));
				slaBean.setModuleName(rst.getString("server_name"));
				slaBean.setReferenceId(rst.getLong("reference_id"));
				//slaBean.setEncryptedUID(rst.getString("encrypted_server_id"));
				slaBean.setEncryptedUID(rst.getString("server_id"));
				
				slaBean.setLevel(rst.getString("level"));
				slaBean.setCounter(rst.getString("counter"));
				slaBean.setThresoldOperator(rst.getString("thresold_operator"));
				slaBean.setThresoldValue(rst.getDouble("thresold_value"));
				
				slaBean.setNotifyBy(rst.getString("notify_by"));
				slaBean.setEmailIds(rst.getString("email_ids"));
				slaBean.setMobileNumbers(rst.getString("mobile_numbers"));
				
				slaBean.setCapaURL(rst.getString("capa_url"));
				slaBean.setURLMethod(rst.getString("url_method"));
				slaBean.setURLParameters(rst.getString("url_parameters"));
				
				alSLAs.add(slaBean);
			}
		} catch(Exception e) {
			LogManager.errorLog(e);
			LogManager.infoLog("sbQuery : "+sbQuery.toString());
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
			
			slaBean = null;
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
		
		return alSLAs;
	}
	
	public void insertSLALog(Connection con, SLABean slaBean){
		PreparedStatement preStmt = null;
		
		try {
			preStmt = con.prepareStatement("INSERT INTO sla_breach_history (subject, description, sla_id, module_type, reference_id, level, counter, thresold_operator, thresold_value, breached_counter_value, notified_by, email_ids, mobile_numbers, capa_url, url_method, url_parameters, breached_on) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			//System.out.println("slaBean.getReferenceId(): "+slaBean.getReferenceId() );
			preStmt.setString(1, "");
			preStmt.setString(2, "");
			preStmt.setLong(3, slaBean.getSLAId());
			preStmt.setString(4, slaBean.getModuleType());
			preStmt.setLong(5, slaBean.getReferenceId());
			preStmt.setString(6, slaBean.getLevel());
			preStmt.setString(7, slaBean.getCounter());
			preStmt.setString(8, slaBean.getThresoldOperator());
			preStmt.setDouble(9, slaBean.getThresoldValue());
			preStmt.setDouble(10, slaBean.getCounterValue());
			preStmt.setString(11, slaBean.getNotifyBy());
			preStmt.setString(12, slaBean.getEmailIds());
			preStmt.setString(13, slaBean.getMobileNumbers());
			preStmt.setString(14, slaBean.getCapaURL());
			preStmt.setString(15, slaBean.getURLMethod());
			preStmt.setString(16, slaBean.getURLParameters());
			preStmt.setTimestamp(17, new Timestamp(slaBean.getBreachedOn().getTime()) );
			
			preStmt.executeUpdate();
			
		} catch (Exception e) {
			LogManager.errorLog(e);
		} finally {
			DataBaseManager.close(preStmt);
			preStmt = null;
		}
	}
}
