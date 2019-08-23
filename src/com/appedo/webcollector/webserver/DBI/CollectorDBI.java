package com.appedo.webcollector.webserver.DBI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.UUID;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Collection operation related Database Interface layer.
 * This will do the general operations related to the Application, Server or Database.
 * 
 * @author Ramkumar
 *
 */
public class CollectorDBI {
	
	public String[] strCounterTables = {"tomcat_performance_counter", "jboss_performance_counter", "msiis_performance_counter", "linux_performance_counter", "windows_performance_counter", "mysql_performance_counter", "mssql_performance_counter", "tomcat_profiler" };
	
	/**
	 * Returns the UID of the Module, which can be Application, Server, Database or other, in the encrypted format.
	 * 
	 * @param con
	 * @param strGUID
	 * @return
	 * @throws Exception
	 *
	public String getEncryptedModuleUID(Connection con, String strGUID) throws Exception {
		String strUID = "";
		
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			//String strQuery = "SELECT encrypted_application_id FROM applicationmaster WHERE guid = '"+strGUID+"'";
			strQuery = "SELECT uid FROM module_master WHERE guid = '"+strGUID+"'";
			
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			while( rst.next() ){
				//strUID = rst.getString("encrypted_application_id");
				strUID = rst.getString("uid");
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return strUID;
	}
	
	/**
	 * Returns the UID of the Module, which can be Application, Server, Database or other, for the given encrypted UID.
	 * 
	 * @param con
	 * @param strEncryptedUID
	 * @return
	 * @throws Exception
	 */
	public long getModuleUID(Connection con, String strGUID) throws Exception {
		long lUID = -1l;
		
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			strQuery = "SELECT uid FROM module_master WHERE guid = '"+strGUID+"'";
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			while( rst.next() ){
				lUID = rst.getLong("uid");
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return lUID;
	}
	
	public JSONObject getAmazonSecurityKeys(Connection con) throws Exception {
		JSONObject joAWSKeys = null;
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			strQuery = "SELECT access_key, secret_access_key from amazon_instance_config";
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			joAWSKeys = new JSONObject();
			
			if( rst.next() ){
				joAWSKeys.put("access_key", rst.getString("access_key"));
				joAWSKeys.put("secret_access_key", rst.getString("secret_access_key"));
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		return joAWSKeys;

	}
	
	public void insertFileTransationData(Connection con, long uid, String bucketName, String fileName, String s3Link, long installed_app_on) throws Exception {
		
		StringBuilder sbQuery = new StringBuilder();
		PreparedStatement pstmt = null;
		
		try {
			sbQuery .append("INSERT INTO installed_application_status_").append(uid)
					.append(" (uid, file_name, bucket_name, created_on, installed_app_on ) VALUES (?, ?, ?, now(), to_timestamp(?)) ");
			pstmt = con.prepareStatement(sbQuery.toString());
			
			pstmt.setLong(1, uid);
			pstmt.setString(2, fileName);
			pstmt.setString(3, bucketName);
			pstmt.setLong(4, installed_app_on/1000);
			
			pstmt.executeUpdate();
			
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} 
		
	}
	
	public void insertFileTransationDataOnFileCompare(Connection con, long uid, String bucketName, String fileName, String displayName, long installed_app_on) throws Exception {
		
		StringBuilder sbQuery = new StringBuilder();
		PreparedStatement pstmt = null;
		
		try {
			sbQuery .append("INSERT INTO file_list_status_").append(uid)
					.append(" (uid, file_name, bucket_name, display_name, created_on, installed_app_on ) VALUES (?, ?, ?, ?, now(), to_timestamp(?)) ");
			pstmt = con.prepareStatement(sbQuery.toString());
			
			pstmt.setLong(1, uid);
			pstmt.setString(2, fileName);
			pstmt.setString(3, bucketName);
			pstmt.setString(4, displayName);
			pstmt.setLong(5, installed_app_on/1000);
			
			pstmt.executeUpdate();
			
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} 
		
	}
	
	public JSONObject getModuleDetails(Connection con, String strGUID) throws Exception {
		JSONObject joModuleDetails = null;
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			strQuery = "SELECT uid, user_id FROM module_master WHERE guid = '"+strGUID+"'";
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			joModuleDetails = new JSONObject();
			
			while( rst.next() ){
				joModuleDetails.put("uid", rst.getLong("uid"));
				joModuleDetails.put("userId", rst.getLong("user_id"));
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return joModuleDetails;
	}
		
	public void updateAgentProgressStatus(Connection con, String UUID, String strModuleName) throws Exception{
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		try {
			//sbQry.append("UPDATE server_information SET agentinprogress = '").append(joAgentProgressStatus.toString()).append("'::jsonb WHERE system_uuid = '").append(UUID).append("'");
			/*sbQry	.append("UPDATE server_information SET AgentInProgress = (jsonb_insert(to_jsonb(AgentInProgress), '{").append(strModuleName).append("}', ")
					.append("'{\"InProgress\":true,\"Message\":\"\"}'::jsonb, false))::json WHERE system_uuid = ?");*/
			sbQry	.append("UPDATE server_information SET AgentInProgress = (jsonb_set(to_jsonb(AgentInProgress), '{").append(strModuleName).append("}', ")
			.append("'{\"InProgress\":true,\"Message\":\"\"}'::jsonb, true))::json WHERE system_uuid = ?");
			pstmt = con.prepareStatement(sbQry.toString());
			pstmt.setString(1, UUID);
			pstmt.execute();
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
	}
	
	public void DeleteAgentProgressJson(Connection con, String UUID, String strModuleName) throws Exception{
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		try {
			sbQry.append("UPDATE server_information SET agentinprogress = agentinprogress::jsonb - '").append(strModuleName).append("' WHERE system_uuid = '").append(UUID).append("'");
			pstmt = con.prepareStatement(sbQry.toString());
			pstmt.execute();
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
	}
	
	public void updateAgentMessage(Connection con, String UUID, String strModuleName, String strMessage) throws Exception{
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		try {
			sbQry.setLength(0);
			sbQry.append("UPDATE server_information SET agentinprogress = (jsonb_set(to_jsonb(AgentInProgress), '{").append(strModuleName).append(",Message}', ")
				.append("'\"").append(strMessage.replaceAll("\"", "")).append("\"', false))::json WHERE system_uuid = '").append(UUID).append("'");
			pstmt = con.prepareStatement(sbQry.toString());
			pstmt.execute();
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
	}
		
	public long getSystemIdV2(Connection con, String UUID) throws Exception{
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		StringBuilder sbQuery = new StringBuilder();
		long lSystemId = -1L;
		try{
			sbQuery.append("select system_id from server_information WHERE system_uuid = ?");
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setString(1, UUID);
			rst = pstmt.executeQuery();
			if( rst.next() ) {
				lSystemId = rst.getLong("system_id");
			}
			
		} catch (Exception e) {
			LogManager.infoLog(sbQuery.toString());
			LogManager.errorLog(e);
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy( sbQuery );
			sbQuery= null;
		}
		return lSystemId;
	}
	
	public JSONObject getSystemData(Connection con, String UUID, String strModuleType, String OSType) throws Exception{
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		String strModuleName = "";
		StringBuilder sbQuery = new StringBuilder();
		JSONObject joReturn = null;
		try{
			sbQuery.append("select system_name, system_number, system_id, user_id from server_information WHERE ");
			if(OSType.equals("WINDOWS")) {
				sbQuery.append("system_uuid = ?");
			}else {
				sbQuery.append("apd_uuid = ?");
			}
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setString(1, UUID);
			rst = pstmt.executeQuery();
			if( rst.next() ) {
				joReturn = new JSONObject();
				//strModuleName = rst.getString("system_name")+"_"+strModuleType+"_"+rst.getString("system_number");
				strModuleName = rst.getString("system_name")+"_"+strModuleType;
				joReturn.put("moduleName", strModuleName);
				joReturn.put("systemId", rst.getLong("system_id"));
				joReturn.put("userId", rst.getLong("user_id"));
			}
			
		} catch (Exception e) {
			LogManager.infoLog(sbQuery.toString());
			
			LogManager.errorLog(e);
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy( sbQuery );
			sbQuery= null;
		}
		return joReturn;
	}
	
	/**
	 * 
	 * @param con
	 * @param UUID
	 * @param strModuleName
	 * @return agent Inprogress is true or false
	 * @throws Exception
	 */
	public JSONObject agentProgressStatus(Connection con, String UUID, String strModuleName) throws Exception{
		//boolean bAgentInProgress = false;
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		JSONObject joAgentData = null;
		StringBuilder sbQuery = new StringBuilder();
		
		try{
			
			sbQuery	.append("select AgentInprogress -> '").append(strModuleName).append("' ->> 'InProgress' AS InProgress, ")
					.append("AgentInprogress -> '").append(strModuleName).append("' ->> 'Message' AS Message ")
					.append("from server_information WHERE system_uuid = ?");
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setString(1, UUID);
			rst = pstmt.executeQuery();
			if( rst.next() ) {
				joAgentData = new JSONObject();
				joAgentData.put("InProgress", rst.getBoolean("InProgress"));
				joAgentData.put("Message", rst.getString("Message"));
			}
			
		} catch (Exception e) {
			LogManager.infoLog(sbQuery.toString());
			LogManager.errorLog(e);
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy( sbQuery );
			sbQuery= null;
		}
		return joAgentData;
	}
		
	/**
	 * Gets APM license details for the user from `userwise_lic_monthwise` table
	 * 
	 * @param con
	 * @param loginUserBean
	 * @return
	 * @throws Exception
	 */
	public JSONObject getAPMUserLicenseDetails(Connection con, long lUser_Id) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		
		StringBuilder sbQuery = new StringBuilder();
		
		JSONObject joAPMLicenseDetails = null;
		
		try {
			sbQuery.append("select * from get_APM_License_Details(?)");
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setLong(1, lUser_Id);
			rst = pstmt.executeQuery();
			if( rst.next() ) {
				joAPMLicenseDetails = new JSONObject();
				joAPMLicenseDetails.put("apm_max_agents", rst.getInt("apm_max_agents"));
				joAPMLicenseDetails.put("apm_max_counters", rst.getInt("apm_max_counters"));
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			LogManager.errorLog(sbQuery.toString());
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy( sbQuery );
			sbQuery= null;
		}
		return joAPMLicenseDetails;
	}
	
	/**
	 * gets user added total no. of modules 
	 * 
	 * @param con
	 * @param loginUserBean
	 * @return
	 * @throws Exception
	 */
	public long getUserAddedModulesCount(Connection con, long lUser_Id) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		
		StringBuilder sbQuery = new StringBuilder();

		// total modules default `0`, since `-1` means user can add with no limits 
		long lTotalUserModulesCount = 0;
		
		try {
			sbQuery	.append("SELECT count(*) AS total_user_modules ")
					.append("FROM module_master ")
					.append("WHERE user_id = ? AND module_code IN ('APPLICATION','SERVER','DATABASE')");

			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setLong(1, lUser_Id);
			rst = pstmt.executeQuery();
			if(rst.next()) {
				lTotalUserModulesCount = rst.getLong("total_user_modules");
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy(sbQuery);
			sbQuery= null;
		}
		
		return lTotalUserModulesCount;
	}
	
	/**
	 * Returns the SystemId of the given UUID.
	 * 
	 * @param con
	 * @param strUUID
	 * @return
	 * @throws Exception
	 */
	public JSONObject getSystemDetails(Connection con, String strUUID) throws Exception {
		long SystemId = -1l;
		
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		JSONObject joSystemData = null;
		try{
			joSystemData = new JSONObject();
			strQuery = "SELECT system_id, user_id, system_name, system_number FROM server_information WHERE system_uuid = '"+strUUID+"'";
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			if( rst.next() ){
				//SystemId = rst.getLong("system_id");
				joSystemData.put("SystemId", rst.getLong("system_id"));
				joSystemData.put("userId", rst.getLong("user_id"));
				joSystemData.put("system_name", rst.getString("system_name"));
				joSystemData.put("system_number", rst.getString("system_number"));
			}else{
				joSystemData.put("SystemId", SystemId);
			}
			
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return joSystemData;
	}
	
	public String getGUID(Connection con, String strUUID, long lSystemId) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		StringBuilder sbQuery = new StringBuilder();
		String strGUID = "";
		try{
			sbQuery.append("SELECT guid from module_master WHERE system_id = ? AND client_unique_id = ? AND module_code = 'NETSTACK'");
			
			pstmt = con.prepareStatement( sbQuery.toString() );
			pstmt.setLong(1, lSystemId);
			pstmt.setString(2, strUUID);
			rst = pstmt.executeQuery();
			
			if(rst.next()){
				strGUID = rst.getString("guid");
			}
		}catch(Exception e){
			LogManager.errorLog(e);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		return strGUID;
	}
	
	public String addModules(Connection con, String strUUID, JSONObject joSystemData, String strEid) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		
		String strGUID = "", strModuleNameDesc;
		
		StringBuilder sbQuery = new StringBuilder();
				
		//long lUid = -1L;
		
		try {
			sbQuery	.append("WITH new_netstack_module AS ( ")
					.append("  INSERT INTO module_master (user_id, guid, module_code, counter_type_version_id, module_name, description, created_by, created_on, e_id, client_unique_id, system_id, module_type, user_status) ")
					.append("  VALUES (?, ?, ?, ?, ?, ?, ?, now(), ?, ?, ?, ?, ?) ")
					.append("  RETURNING * ")
					.append(") ")
					.append("SELECT * FROM add_net_stack_collector_table( (SELECT uid from new_netstack_module), ?)");
					
			//pstmt = con.prepareStatement(strQuery, PreparedStatement.RETURN_GENERATED_KEYS);
			pstmt = con.prepareStatement( sbQuery.toString() );
			
			strGUID = UUID.randomUUID().toString();
			strModuleNameDesc = joSystemData.getString("system_name")+"_"+"NETSTACK"+"_"+joSystemData.getString("system_number");
			
			pstmt.setLong(1, joSystemData.getLong("userId"));
			pstmt.setString(2, strGUID);
			pstmt.setString(3, "NETSTACK");
			pstmt.setInt(4, 0);
			pstmt.setString(5, strModuleNameDesc);
			pstmt.setString(6, strModuleNameDesc);
			pstmt.setLong(7, joSystemData.getLong("userId"));
			if(strEid != null && !strEid.isEmpty()) {
				pstmt.setLong(8, Long.parseLong(strEid));
			}else {
				pstmt.setNull(8, java.sql.Types.INTEGER);
			}
			pstmt.setString(9, strUUID);
			pstmt.setLong(10, joSystemData.getLong("SystemId"));
			pstmt.setString(11, "NETSTACK");
			pstmt.setString(12, "Running");
			pstmt.setLong(13, joSystemData.getLong("userId"));
			
			//pstmt.executeUpdate();
			rst = pstmt.executeQuery();
			if(rst.next()) {
				//lUid = rst.getLong("uid");
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
			throw e;
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			sbQuery = null;
		}
		
		return strGUID;
	}
	
	public long getSystemId(Connection con, String strSystemUUID) throws Exception {
		long lSystemId = -1l;
		
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			strQuery = "SELECT system_id FROM server_information WHERE system_uuid = '"+strSystemUUID+"'";
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			while( rst.next() ){
				lSystemId = rst.getLong("system_id");
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return lSystemId;
	}
	
	/**
	 * Get the UID (primary key) for the given GUID.
	 * 
	 * @param con
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public long getApplicationUID(Connection con, String strGUID) throws Exception {
		Statement stmt = null;
		ResultSet rst = null;
		
		StringBuilder sbQuery = new StringBuilder();
		
		long lUID = -1l;
		
		try {
			// TODO RAM; Keep the UID in HashMap, if it is not available run the query.
			
			sbQuery.append("SELECT uid FROM module_master WHERE guid = '").append(strGUID).append("' ");
			stmt = con.createStatement();
			rst = stmt.executeQuery(sbQuery.toString());
			
			while( rst.next() ){
				lUID = rst.getLong("uid");
			}
		} catch (Exception e) {
			LogManager.errorLog(e, sbQuery);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			UtilsFactory.clearCollectionHieracy(sbQuery);
			sbQuery = null;
		}
		
		return lUID;
	}
	
	/**
	 * To get selected counters from counter master table 
	 * @param con
	 * @param joLicense 
	 * @param strGUID
	 * @return 
	 */
	public JSONArray getConfigCounters(Connection con, long lUID, JSONObject joLicense) { 
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		JSONArray jaCounters = new JSONArray();
		JSONObject joCounter = null;
		
		try {
			sbQry.append("SELECT counter_id, query_string, top_process_query_string, execution_type, is_delta, is_top_process, is_static_counter, max_value_counter_id FROM counter_master_")
				.append(lUID)
				.append(" WHERE is_selected = true")
				.append(" ORDER BY counter_id ");
				
			if(joLicense.getInt("apm_max_counters") != -1){
				sbQry.append("LIMIT ").append(joLicense.getInt("apm_max_counters"));
			}
			
			pstmt = con.prepareStatement(sbQry.toString());
			rs = pstmt.executeQuery();
			
			while(rs.next()) {
				
				joCounter = new JSONObject();
				joCounter.put("counter_id", rs.getString("counter_id"));
				joCounter.put("query", rs.getString("query_string"));
				joCounter.put("top_process_query", rs.getString("top_process_query_string"));
				joCounter.put("executiontype", rs.getString("execution_type"));
				joCounter.put("isdelta", rs.getBoolean("is_delta"));
				joCounter.put("isTopProcess", rs.getBoolean("is_top_process"));
				joCounter.put("isStaticCounter", rs.getBoolean("is_static_counter"));
				joCounter.put("maxValueCounterId", rs.getString("max_value_counter_id"));
				
				jaCounters.add(joCounter);
				
				joCounter = null;
			}
		} catch(Exception ex) {			
			LogManager.errorLog(ex);
		} finally {
			
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			DataBaseManager.close(rs);
			rs = null;
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
		
		return jaCounters;
	}
	
	/**
	 * To get selected counters from counter master table 
	 * @param con
	 * @param joLicense 
	 * @param strGUID
	 * @return 
	 */
	public JSONArray getConfigCountersV2(Connection con, long lUID) { 
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		JSONArray jaCounters = new JSONArray();
		JSONObject joCounter = null;
		
		try {
			sbQry.append("SELECT counter_id, query_string, top_process_query_string, execution_type, is_delta, is_top_process, is_static_counter, max_value_counter_id FROM counter_master_")
				.append(lUID)
				.append(" WHERE is_selected = true")
				.append(" ORDER BY counter_id ");
			pstmt = con.prepareStatement(sbQry.toString());
			rs = pstmt.executeQuery();
			
			while(rs.next()) {
				
				joCounter = new JSONObject();
				joCounter.put("counter_id", rs.getString("counter_id"));
				joCounter.put("query", rs.getString("query_string"));
				joCounter.put("top_process_query", rs.getString("top_process_query_string"));
				joCounter.put("executiontype", rs.getString("execution_type"));
				joCounter.put("isdelta", rs.getBoolean("is_delta"));
				joCounter.put("isTopProcess", rs.getBoolean("is_top_process"));
				joCounter.put("isStaticCounter", rs.getBoolean("is_static_counter"));
				joCounter.put("maxValueCounterId", rs.getString("max_value_counter_id"));
				
				jaCounters.add(joCounter);
				
				joCounter = null;
			}
		} catch(Exception ex) {			
			LogManager.errorLog(ex);
		} finally {
			
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			DataBaseManager.close(rs);
			rs = null;
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
		
		return jaCounters;
	}
	/**
	 * 
	 * @param con
	 * @param lUID
	 * @param nParentCounterId
	 * @return
	 * @throws Exception
	 */
	public JSONObject getParentCounterDetails(Connection con, long lUID, int nParentCounterId) throws Exception {
		
		PreparedStatement pstmt = null;
		ResultSet rst = null;

		StringBuilder sbQuery = new StringBuilder();

		JSONObject joParentCounterDetails = null;
		
		// Get counter template id & user id
		try {
			sbQuery	.append("SELECT counter_template_id, user_id, case when instance_name is null then 'NA' else instance_name end, counter_name, category, query_string FROM counter_master_").append(lUID)
					.append(" WHERE counter_id = ").append(nParentCounterId);
			pstmt = con.prepareStatement(sbQuery.toString());
			rst = pstmt.executeQuery();
			
			if( rst.next() ) {
				joParentCounterDetails = new JSONObject();
				
				joParentCounterDetails.put("uid", lUID);
				joParentCounterDetails.put("user_id", rst.getLong("user_id"));
				joParentCounterDetails.put("counter_id", nParentCounterId);
				joParentCounterDetails.put("counter_template_id", rst.getLong("counter_template_id"));				
				joParentCounterDetails.put("instance_name", rst.getString("instance_name"));
				joParentCounterDetails.put("counter_name", rst.getString("counter_name"));
				joParentCounterDetails.put("category", rst.getString("category"));
				joParentCounterDetails.put("queryString", rst.getString("query_string"));
			}
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			DataBaseManager.close(rst);
			rst = null;
			UtilsFactory.clearCollectionHieracy(sbQuery);
		}
		
		return joParentCounterDetails;
	}
	
	
	
	
	/**
	 * To insert child counters of parent counter
	 * @param con
	 * @param joMasterCounter
	 * @param lUID
	 * @param nParentCounterId
	 * @throws Exception
	 */
	public void insertCounterMasterTable(Connection con, JSONObject joMasterCounter, long lUID, int nParentCounterId) throws Exception {
		
		PreparedStatement pstmt = null;
		StringBuilder sbQry = new StringBuilder();
		ResultSet rs = null;
		long lCounterTemplateId = 0l;
		long lUserId = 0l;
		
		try {
			// Get counter template id & user id 
			try {
				sbQry.append("select counter_template_id,user_id from counter_master_").append(lUID)
					.append(" where counter_id = ").append(nParentCounterId);
				pstmt = con.prepareStatement(sbQry.toString());
				rs = pstmt.executeQuery();
				
				while( rs.next() ) {
					lCounterTemplateId = Long.parseLong(rs.getString("counter_template_id")) ;
					lUserId = Long.parseLong(rs.getString("user_id"));
				}
			} catch(Exception ex) {			
				LogManager.errorLog(ex);
			} finally {
				DataBaseManager.close(pstmt);
				pstmt = null;
				
				DataBaseManager.close(rs);
				rs = null;
				
				UtilsFactory.clearCollectionHieracy(sbQry);
			}
			
			sbQry .append("insert into counter_master_")
					.append(lUID)
					.append("(counter_template_id, ")
					.append("user_id, ")
					.append("guid, ")
					.append("query_string, ")
					.append("execution_type, ")
					.append("counter_name, ")
					.append("category, ")
					.append("is_selected, ")
					.append("has_instance, ")
					.append("is_enabled, ")
					.append("last_date_sent_to_agent, ")
					.append("created_on, ")
					.append("created_by, instance_name, display_name) ")
					
					.append("values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now(), ?, ?, ?)");
			
			pstmt = con.prepareStatement(sbQry.toString());
			
			pstmt.setLong(1, lCounterTemplateId);
			pstmt.setLong(2, lUserId);
			pstmt.setString(3, joMasterCounter.getString("guid"));
			pstmt.setString(4, joMasterCounter.getString("queryString"));
			pstmt.setString(5, joMasterCounter.getString("executionType"));
			pstmt.setString(6, joMasterCounter.getString("counterName"));
			pstmt.setString(7, joMasterCounter.getString("category"));			
			pstmt.setBoolean(8, joMasterCounter.getBoolean("is_selected"));			
			pstmt.setBoolean(9, joMasterCounter.getBoolean("has_instance"));
			pstmt.setBoolean(10, joMasterCounter.getBoolean("is_enabled"));
			pstmt.setLong(11, lUserId);
			pstmt.setString(12, joMasterCounter.getString("instance_name"));
			pstmt.setString(13, joMasterCounter.getString("display_name"));
			
			pstmt.execute();
		}catch(Exception e){
			LogManager.errorLog(e);
			throw e;
		}finally{
			DataBaseManager.close(pstmt);
			pstmt = null;
			sbQry = null;
		}
	}
	
	/**
	 * to update counter master table for given parent counter
	 * @param con
	 * @param lUID
	 * @param nParentCounterId
	 */
	public void updateParentCounter(Connection con, JSONObject joParentCounterDetails) {
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		
		try {
			if(joParentCounterDetails.getString("instance_name").equals("NA")) {
				sbQry.append("UPDATE counter_master_").append(joParentCounterDetails.getLong("uid"))
					.append(" SET is_selected = false, is_enabled = false WHERE counter_id = ").append(joParentCounterDetails.getLong("counter_id"));
				
				pstmt = con.prepareStatement(sbQry.toString());
			} else {
				sbQry.append("UPDATE counter_master_").append(joParentCounterDetails.getLong("uid"))
					.append(" SET has_instance = false, query_string = ? WHERE counter_id = ").append(joParentCounterDetails.getLong("counter_id"));
				
				pstmt = con.prepareStatement(sbQry.toString());
				pstmt.setString(1, joParentCounterDetails.getString("queryString").replaceAll("TRUE,", "FALSE,"));
			}
			pstmt.execute();
			
			LogManager.logChildCounters("counter_master_"+joParentCounterDetails.getLong("uid")+"'s "+joParentCounterDetails.getLong("counter_id")+" counter's parent details is updated");
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;	
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
	}
	
	/**
	 * TODO enable_backup
	 * 
	 * Backup the counter & profiler tables into respective archive tables.
	 * This will backup the data older than 24 hours. And this will be running for each one hour.
	 * 
	 * @param con
	 *
	public void backupPreviousDayRecords(Connection con) {
		String qryResponseMaxId = null;
		String qryCounterMaxId = null;
		String qryBackup = null;
		String qryCounterDelete = null;
		String qryResponseDelete = null;
		
		PreparedStatement psResponseMaxId = null;
		PreparedStatement psCounterMaxId = null;
		PreparedStatement psBackup = null;
		PreparedStatement psCounterDelete = null;
		PreparedStatement psResponseDelete = null;
		ResultSet rstRespMaxId = null, rstCounterMaxId = null;
		
		long lRespMaxId = 0l, lCounterMaxId = 0l;
		int nIns = 0;
		
		try{
			qryResponseMaxId = "SELECT MAX(response_id) AS max_response_id FROM agent_response WHERE received_on < ( now() - INTERVAL '24 HOUR' )";
			qryCounterMaxId = "SELECT MAX(id) AS max_id FROM TABLENAME src WHERE agent_response_id <= ?";
			qryBackup = "INSERT INTO TABLENAME_archive SELECT * FROM TABLENAME WHERE id <= ?";
			qryCounterDelete = "DELETE FROM TABLENAME WHERE id <= ?";
			qryResponseDelete = "DELETE FROM agent_response WHERE response_id <= ?";
			
			psResponseMaxId = con.prepareStatement(qryResponseMaxId);
			psResponseDelete = con.prepareStatement(qryResponseDelete);
			
			
			// get the response table's previous day's max response_id
			rstRespMaxId = psResponseMaxId.executeQuery();
			if( rstRespMaxId.next() ){
				lRespMaxId = rstRespMaxId.getLong("max_response_id");
				//System.out.println("DBBKUP: lRespMaxId: "+lRespMaxId);
			}
			
			for( int i = 0; i < strCounterTables.length; i++ ){
				try{
					// get counter table's previous day's max response_id
					//System.out.println("DBBKUP: qryCounterMaxId: +"+qryCounterMaxId.replaceAll("TABLENAME", strCounterTables[i])+" <> "+lRespMaxId);
					psCounterMaxId = con.prepareStatement( qryCounterMaxId.replaceAll("TABLENAME", strCounterTables[i]) );
					psCounterMaxId.setLong(1, lRespMaxId);
					rstCounterMaxId = psCounterMaxId.executeQuery();
					if( rstCounterMaxId.next() ){
						lCounterMaxId = rstCounterMaxId.getLong("max_id");
						//System.out.println("DBBKUP: lCounterMaxId: "+lCounterMaxId);
					}
					
					// insert the data into archive table
					//System.out.println("DBBKUP: qryBackup: "+qryBackup.replaceAll("TABLENAME", strCounterTables[i])+" <> "+lCounterMaxId);
					psBackup = con.prepareStatement( qryBackup.replaceAll("TABLENAME", strCounterTables[i]) );
					psBackup.setLong(1, lCounterMaxId);
					nIns = psBackup.executeUpdate();
					//System.out.println("qryBackup ins: "+nIns);
					
					// delete the data counter table
					//System.out.println("DBBKUP: qryCounterDelete: "+qryCounterDelete.replaceAll("TABLENAME", strCounterTables[i])+" <> "+lCounterMaxId);
					psCounterDelete = con.prepareStatement( qryCounterDelete.replaceAll("TABLENAME", strCounterTables[i]) );
					psCounterDelete.setLong(1, lCounterMaxId);
					nIns = psCounterDelete.executeUpdate();
					//System.out.println("qryCounterDelete ins: "+nIns);
					
				} catch (Exception ex) {
					LogManager.errorLog(ex);
				} finally {
					DataBaseManager.close(rstCounterMaxId);
					rstCounterMaxId = null;
					
					DataBaseManager.close(psCounterMaxId);
					psCounterMaxId = null;
					
					DataBaseManager.close(psBackup);
					psBackup = null;
					
					DataBaseManager.close(psCounterDelete);
					psCounterDelete = null;
				}
			}
			
			// delete the response data
			psResponseDelete.setLong(1, lRespMaxId);
			nIns = psResponseDelete.executeUpdate();
			//System.out.println("qryResponseDelete ins: "+nIns);
			
		} catch (Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(rstRespMaxId);
			rstRespMaxId = null;
			DataBaseManager.close(rstCounterMaxId);
			rstCounterMaxId = null;
			
			DataBaseManager.close(psResponseMaxId);
			psResponseMaxId = null;
			DataBaseManager.close(psResponseDelete);
			psResponseDelete = null;
			DataBaseManager.close(psCounterMaxId);
			psCounterMaxId = null;
			DataBaseManager.close(psBackup);
			psBackup = null;
			DataBaseManager.close(psCounterDelete);
			psCounterDelete = null;
			
			qryResponseMaxId = null;
			qryCounterMaxId = null;
			qryBackup = null;
			qryCounterDelete = null;
			qryResponseDelete = null;
			
			lRespMaxId = 0l;
			lCounterMaxId = 0l;
			nIns = 0;
		}
	}*/
	
	/**
	 * To update the is_agent_updated & last_date_sent_to_agent in counter master table 
	 * @param con
	 * @param strGUID
	 */
	public void updateStatusOfCounterMaster(Connection con, String strGUID) {
		
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		
		try {
			long lUid = getModuleUID(con, strGUID);
			sbQry	.append("UPDATE counter_master_").append(lUid)
					.append(" SET last_date_sent_to_agent = now(), is_agent_updated = false WHERE is_agent_updated = true");
			pstmt = con.prepareStatement(sbQry.toString());
			pstmt.execute();
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
		
	}
	
	public JSONObject getUserLicense(Connection con, String strGUID) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;

		StringBuilder sbQuery = new StringBuilder();

		JSONObject joUserLic = null;

		try {
			sbQuery.append("SELECT um.user_id, um.license_level FROM module_master mm INNER JOIN usermaster um ON um.user_id = mm.user_id AND mm.guid = ?");
			
			pstmt = con.prepareStatement( sbQuery.toString() );
			pstmt.setString(1, strGUID);
			rst = pstmt.executeQuery();
			
			if (rst.next()) {
				joUserLic = new JSONObject();
				joUserLic.put("user_id", rst.getLong("user_id"));
				joUserLic.put("license_level", rst.getString("license_level"));
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		return joUserLic;
	}
	
	public HashMap<String, Object> getModuleType(Connection con, String strUUID, long lSystemId, HashMap<String, Object> hmKeyValues) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;

		StringBuilder sbQuery = new StringBuilder();

		JSONObject joModuleType = null;
		JSONArray jaModuletype = null;

		HashMap<String, Object> hmKeyValue = hmKeyValues;
		try {
			//sbQuery.append("SELECT module_code from module_master WHERE system_id = ? AND client_unique_id = ? AND module_code IN ('APPLICATION','SEREVER','DATABASE')");
			//sbQuery.append("SELECT mm.module_code, vmm.counter_type_name, mm.uid, mm.guid from module_master mm INNER JOIN v_module_master_version vmm ON mm.uid = vmm.uid WHERE mm.system_id = ? AND mm.client_unique_id = ? AND vmm.counter_type_name IN ('MSIIS','MSSQL','Windows')");
			sbQuery.append("SELECT uid, guid, module_code, module_name from module_master WHERE system_id = ? AND client_unique_id = ? AND module_type IN ('MSIIS','MSSQL','WINDOWS')");
			
			pstmt = con.prepareStatement( sbQuery.toString() );
			pstmt.setLong(1, lSystemId);
			pstmt.setString(2, strUUID);
			rst = pstmt.executeQuery();
			
			jaModuletype = new JSONArray();
			while (rst.next()) {
				joModuleType = new JSONObject();
				//joModuleType.put("CounterType", rst.getString("counter_type_name"));
				//joModuleType.put("moduleType", rst.getString("module_code"));
				joModuleType.put("guid", rst.getString("guid"));
				joModuleType.put("uid", rst.getString("uid"));
				joModuleType.put("message", "");
				if(rst.getString("module_code").endsWith("SERVER")){
					hmKeyValue.put("WINDOWS", joModuleType);
				}else if(rst.getString("module_code").endsWith("APPLICATION")){
					hmKeyValue.put("MSIIS", joModuleType);
				}else if(rst.getString("module_code").endsWith("DATABASE")){
					hmKeyValue.put("MSSQL", joModuleType);
				}
				//hmKeyValue.put(rst.getString("counter_type_name"), joModuleType);
				jaModuletype.add(joModuleType);
				joModuleType = null;
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		//return jaModuletype;
		return hmKeyValue;
	}
	
	/**
	 * 
	 * @param con
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public JSONObject getModuleStatus(Connection con, String strGUID) throws Exception{
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		StringBuilder sbQuery = new StringBuilder();
		JSONObject joModuleStatus = null;
		try {
			sbQuery.append("SELECT user_status, uid, module_type FROM module_master WHERE guid = ?");
			pstmt = con.prepareStatement( sbQuery.toString() );
			pstmt.setString(1, strGUID); 
			rst = pstmt.executeQuery();
			joModuleStatus = new JSONObject();
			if (rst.next()) {
				
				joModuleStatus.put("user_status", rst.getString("user_status"));
				joModuleStatus.put("uid", rst.getString("uid"));
				joModuleStatus.put("module_type", rst.getString("module_type"));
				//strModuleStatus = rst.getString("user_status");
			}else{
				joModuleStatus.put("user_status", "deleted");
			}
		}catch (Exception e) {
			LogManager.errorLog(e);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		//return strModuleStatus;
		return joModuleStatus;
	}
	
	public String getModuleRunningStatus(Connection con, String strGUID) throws Exception{
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		StringBuilder sbQuery = new StringBuilder();
		String strModuleStatus = "";
		try {
			sbQuery.append("SELECT user_status FROM module_master WHERE guid = ?");
			pstmt = con.prepareStatement( sbQuery.toString() );
			pstmt.setString(1, strGUID); 
			rst = pstmt.executeQuery();
			if (rst.next()) {
				strModuleStatus = rst.getString("user_status");			
			}else{
				strModuleStatus = "Kill";
			}
		}catch (Exception e) {
			LogManager.errorLog(e);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		return strModuleStatus;
	}
	
	/**
	 * 
	 * @param con
	 * @param strGUID
	 */
	public void updateModuleStatus(Connection con, String strGUID) {	
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		try {
			sbQry.append("UPDATE module_master SET user_status = 'Running' WHERE guid = ? ");
			pstmt = con.prepareStatement(sbQry.toString());
			pstmt.setString(1, strGUID);
			pstmt.executeUpdate();
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
	}
	
	public void updateJbossInfo(Connection con, String strGUID, JSONObject joJbossInfo) {	
		StringBuilder sbQry = new StringBuilder();
		PreparedStatement pstmt = null;
		try {
			sbQry.append("UPDATE module_master SET description = ?, application_context_name = ? WHERE guid = ? ")
				 .append("AND application_context_name <> ? ");
			pstmt = con.prepareStatement(sbQry.toString());
			pstmt.setString(1, joJbossInfo.getString("moduleName"));
			pstmt.setString(2, joJbossInfo.getString("moduleName"));
			pstmt.setString(3, strGUID);
			pstmt.setString(4, joJbossInfo.getString("moduleName"));
			pstmt.executeUpdate();
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy(sbQry);
		}
	}
	
	public JSONObject getLicenseAPMDetails(Connection con, JSONObject joUserLic) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rst = null;

		StringBuilder sbQuery = new StringBuilder();

		JSONObject joLicenseDetails = null;

		try {
			if( !joUserLic.getString("license_level").equalsIgnoreCase("level0") ){
				sbQuery.append("SELECT apm_max_agents, apm_max_counters FROM userwise_lic_monthwise ")
						.append("WHERE user_id = ? ")
						.append("AND module_type = 'APM' AND start_date::DATE <= now()::DATE ")
						.append("AND end_date::DATE >= now()::DATE");
			} else{
				sbQuery.append("SELECT apm_max_agents, apm_max_counters FROM usermaster WHERE user_id = ?");
			}
			
			pstmt = con.prepareStatement( sbQuery.toString() );
			pstmt.setLong(1, joUserLic.getLong("user_id"));
			rst = pstmt.executeQuery();
			
			if (rst.next()) {
				joLicenseDetails = new JSONObject();
				joLicenseDetails.put("apm_max_agents", rst.getInt("apm_max_agents"));
				joLicenseDetails.put("apm_max_counters", rst.getInt("apm_max_counters"));
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			
			throw e;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		return joLicenseDetails;
	}
	
	public ResultSet collectUpgradeGUID(Connection con) throws Exception {
		
		PreparedStatement pstmt = null;
		StringBuilder sbQuery = new StringBuilder();
		
		try {
			// sbQuery.append("select guid,download_url from module_master where version_updated=false order by uid limit 100");
			sbQuery.append("select guid from module_master where version_updated=false order by uid ");
			pstmt = con.prepareStatement( sbQuery.toString() );		
			
			return pstmt.executeQuery();	
		} catch (Exception e) {
			LogManager.errorLog(e);
			throw e;
		} finally {
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		
	}
	
	public JSONArray getCounters(Connection con, String strGUID) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		StringBuilder sbQuery = new StringBuilder();
		JSONArray jaCounters = null;
		JSONObject joCounter = null;
		
		try {
			jaCounters = new JSONArray();
			
			sbQuery.append("SELECT * FROM counter_master_")
					.append(getModuleId(con, strGUID))
					.append(" WHERE is_selected = true")
					.append(" ORDER BY counter_name");
			pstmt = con.prepareStatement( sbQuery.toString() );		
			rs = pstmt.executeQuery();
			while(rs.next()){
				joCounter = new JSONObject();
				joCounter.put("counter_id", rs.getInt("counter_id"));
				joCounter.put("query", rs.getString("query_string"));
				joCounter.put("executiontype", rs.getString("execution_type"));
				joCounter.put("isdelta", rs.getBoolean("is_delta"));


				jaCounters.add(joCounter);
				joCounter = null;
			}
		} catch (Exception e) {
			LogManager.errorLog(e);
			throw e;
		} finally {
			
			//DataBaseManager.close(pstmt);
			//pstmt = null;
			UtilsFactory.clearCollectionHieracy( sbQuery );
		}
		return jaCounters;
	}
	
	public long getModuleId(Connection con, String strGUID) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		StringBuffer sbQuery = new StringBuffer();
		long lModuleId = -1;
		
		try{
			sbQuery .append("SELECT uid FROM module_master WHERE guid=?");
			
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setString(1, strGUID);
			rs = pstmt.executeQuery();
			if(rs.next()){
				lModuleId = rs.getLong("uid");
			}
		}catch(Exception e){
			LogManager.errorLog(e);
			throw e;
		}finally{
			DataBaseManager.close(rs);
			rs = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
		}
		return lModuleId;
	}
	
	public static String createDailyPartition(PreparedStatement pstmt, long lUID, String strPartitionKey) {
		ResultSet rst = null;
		
		try {
			pstmt.setLong(1, lUID);
			pstmt.setString(2, strPartitionKey);
			
			rst = pstmt.executeQuery();
			while( rst.next() ) {
				strPartitionKey = rst.getString(1);
			}
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
		
		return strPartitionKey;
	}
	
	public JSONArray getNetStackAddedUserIds(Connection con) throws Throwable {
		JSONArray jaUserDetails = new JSONArray();
		
		Statement stmt = null;
		ResultSet rst = null;
		StringBuffer sbQuery = new StringBuffer();
		
		try{
			sbQuery .append("SELECT user_id, uid FROM module_master ")
					.append("WHERE module_code = 'NETSTACK' ")
					.append("  AND last_appedo_received_on >= now() - INTERVAL '").append(Constants.NET_STACK_AGENT_SERVICE_RUNTIME_INTERVAL_MS).append(" ms' ");
			
			stmt = con.createStatement();
			rst = stmt.executeQuery(sbQuery.toString());
			while( rst.next() ) {
				JSONObject joUserBreachDet = new JSONObject();
				joUserBreachDet.put("userId", rst.getLong("user_id"));
				joUserBreachDet.put("uid", rst.getLong("uid"));
				
				jaUserDetails.add(joUserBreachDet);
			}
		} catch(Throwable th) {
			throw th;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(stmt);
			stmt = null;
		}
		
		return jaUserDetails;
	}
	
	public boolean fillNetStackContinuityId(Connection con, long lUID) throws Throwable {
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		StringBuffer sbQuery = new StringBuffer();
		
		boolean bUpdated = false;
		
		try{
			sbQuery .append("SELECT update_continuity_id_net(?)");
			
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setLong(1, lUID);
			rst = pstmt.executeQuery();
			
			if( rst.next() ) {
				bUpdated = rst.getBoolean(1);
			}
		} catch(Throwable th) {
			throw th;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
		}
		
		return bUpdated;
	}
	
	public JSONObject getUserInfo(Connection con, String Encrypted_id, long user_id) throws Exception {
		JSONObject joUserDetails = null;
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			if(Encrypted_id.isEmpty()) {
				strQuery = "SELECT user_id, email_id, first_name FROM usermaster WHERE user_id = "+ user_id;
			}else {
				strQuery = "SELECT user_id, email_id, first_name FROM usermaster WHERE encrypted_user_id = '"+Encrypted_id+"'";
			}
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			joUserDetails = new JSONObject();
			
			while( rst.next() ){
				joUserDetails.put("user_id", rst.getLong("user_id"));
				joUserDetails.put("email_id", rst.getString("email_id"));
				joUserDetails.put("name", rst.getString("first_name"));
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return joUserDetails;
	}
	
	public JSONObject getServerInfo(Connection con, String UUID) throws Exception {
		JSONObject joServerDetails = null;
		String strQuery = null;
		Statement stmt = null;
		ResultSet rst = null;
		
		try{
			strQuery = "SELECT user_id, system_id FROM server_information WHERE apd_uuid = '"+UUID+"'";
			stmt = con.createStatement();
			rst = stmt.executeQuery(strQuery);
			
			joServerDetails = new JSONObject();
			
			if( rst.next() ){
				joServerDetails.put("isExistsUUID", true);
				joServerDetails.put("user_id", rst.getLong("user_id"));
				joServerDetails.put("system_id", rst.getLong("system_id"));
			}else {
				joServerDetails.put("isExistsUUID", false);
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		} finally {
			DataBaseManager.close(rst);
			rst = null;
			
			DataBaseManager.close(stmt);
			stmt = null;
			
			strQuery = null;
		}
		
		return joServerDetails;
	}
	
	/***
	 * 
	 * @param con
	 * @param moduleBean
	 * @throws Exception
	 * 
	 * @return ModuleId
	 */
	public JSONObject getModuleId(Connection con, long systemId, String UUID, String moduleType, String strJmxPort, boolean isContextName) throws Exception{
		PreparedStatement pstmt = null;
		ResultSet rst = null;
		//long lModuleId = -1L;
		StringBuilder sbQuery = new StringBuilder();
		JSONObject joModuleData = new JSONObject();
		try{
			
			sbQuery.append(" SELECT uid, guid from module_master WHERE system_id = ? ");
			if(!isContextName) {
				sbQuery.append(" AND lower(module_type) = lower(?)");
			}else {
				sbQuery.append(" AND lower(application_context_name) = lower(?)");
			}
			if (strJmxPort != null) {
				sbQuery.append(" AND jmx_port = ?");
			}
			pstmt = con.prepareStatement(sbQuery.toString());
			pstmt.setLong(1, systemId);
			pstmt.setString(2, moduleType);
			if (strJmxPort != null) {
				pstmt.setString(3, strJmxPort);
			}
			rst = pstmt.executeQuery();
			
			if(rst.next()){
				//lModuleId = rst.getLong("uid");
				joModuleData.put("lModuleId", rst.getLong("uid"));
				joModuleData.put("moduleGUID", rst.getString("guid"));
				joModuleData.put("isExistsGUID", true);
			}else {
				joModuleData.put("isExistsGUID", false);
			}
		}catch(Exception e){
			LogManager.infoLog(sbQuery.toString());
			LogManager.errorLog(e);
			throw e;
		}finally{
			DataBaseManager.close(rst);
			rst = null;
			DataBaseManager.close(pstmt);
			pstmt = null;
			
			UtilsFactory.clearCollectionHieracy( sbQuery );
			sbQuery= null;
		}
		return joModuleData;
	}
	public long insertServerInformation(Connection con, JSONObject serverInformation, long user_id, String UUID) throws Exception {
		
		StringBuilder sbQuery = new StringBuilder();
		PreparedStatement pstmt = null;
		
		long lSystemId = -1L;
		try {
			sbQuery .append("INSERT INTO server_information (user_id, manufacturer, system_name, system_uuid, system_number, created_on) ")
					.append("VALUES (?, ?, ?, ?, ?, now()) ");
			pstmt = con.prepareStatement(sbQuery.toString(), PreparedStatement.RETURN_GENERATED_KEYS);
	
			pstmt.setLong(1, user_id);
			pstmt.setString(2, serverInformation.getString("Manufacturer"));
			pstmt.setString(3, serverInformation.getString("Product Name"));
			pstmt.setString(4, UUID);
			pstmt.setString(5, serverInformation.getString("Serial Number"));
			
			//pstmt.executeUpdate();
			
			//lSystemId = DataBaseManager.returnKey(pstmt);
			
			lSystemId = DataBaseManager.insertAndReturnKey(pstmt, "system_id");
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
		return lSystemId;
	}
}
