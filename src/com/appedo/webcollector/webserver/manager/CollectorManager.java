package com.appedo.webcollector.webserver.manager;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.AmazonS3APIWrapper;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.JavaProfilerDBI;
import com.appedo.webcollector.webserver.bean.CollectorBean;
import com.appedo.webcollector.webserver.bean.JStackDataBean;
import com.appedo.webcollector.webserver.bean.NetStackDataBean;
import com.appedo.webcollector.webserver.bean.NotificationBean;
import com.appedo.webcollector.webserver.bean.UnifiedCounterDataBean;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Manager which holds the queues, of all the performance counters
 * Singleton class. So no objects can be created.
 * 
 * @author Ramkumar
 *
 */
public class CollectorManager{
	
	// Singleton object, used globally with static getCollectorManager().
	private static CollectorManager collectorManager = new CollectorManager();
	
	// Queue to store performance counter data of agents
	private PriorityBlockingQueue<CollectorBean> pqModulePerformanceCounters = new PriorityBlockingQueue<CollectorBean>();
	private PriorityBlockingQueue<CollectorBean> pqModulePerformanceCountersV1 = new PriorityBlockingQueue<CollectorBean>();
	
	// Queue to store unified agent's counter data
	private PriorityBlockingQueue<UnifiedCounterDataBean> pqUnifiedAgentCounters = new PriorityBlockingQueue<UnifiedCounterDataBean>();
	
	// Queue to store agents's notification data
	private PriorityBlockingQueue<NotificationBean> pqNofificationData = new PriorityBlockingQueue<NotificationBean>();
	
	// Queue to store NetStack's counter data
	private PriorityBlockingQueue<NetStackDataBean> pqNetStackData = new PriorityBlockingQueue<NetStackDataBean>();
	
	// Queue to store JStack's counter data
	private PriorityBlockingQueue<JStackDataBean> pqJStackData = new PriorityBlockingQueue<JStackDataBean>();
	
	private PriorityBlockingQueue<String> pqJavaProfiler = null;
	private PriorityBlockingQueue<String> pqDotNetProfiler = null;
	private PriorityBlockingQueue<String> pqPGSlowQueries = null;
	private PriorityBlockingQueue<String> mssqlSlowQueries = null;
	private PriorityBlockingQueue<String> mssqlProcedures = null;
	private PriorityBlockingQueue<String> mysqlSlowQueries = null;
	private PriorityBlockingQueue<String> oracleSlowQueries = null;
	private PriorityBlockingQueue<String> beatIds = null;
	
	private HashMap<String, Object[]> hmLatestCounters = null;
	
	private static HashMap<String,Object>  hmCountersBean = new HashMap<String,Object>();
	private static HashMap<String,Object>  hmSlaCountersBean = new HashMap<String,Object>();
	public static HashMap<String,Object>  hmUpgradeAgentBean = new HashMap<String,Object>();
	
	public Object[] getLatestCounters(String strKey) {
		return hmLatestCounters.get(strKey);
	}
	public boolean isLatestCountersContains(String strKey) {
		return hmLatestCounters.containsKey(strKey);
	}
	public void addLatestCounter(String strKey, Object[] objArr) {
		this.hmLatestCounters.put(strKey, objArr);
	}
	
	

	/**
	 * @return the hmCountersBean
	 */
	public static JSONArray getCounters(String strGUID) {		
		return (JSONArray) hmCountersBean.get(strGUID);
	}
	
	/**
	 * @return the hmCountersBean
	 */
	public static JSONArray getSlaCounters(String strGUID) {
		return (JSONArray) hmSlaCountersBean.get(strGUID);
	}

	/**
	 * @param hmCountersBean the hmCountersBean to set
	 */
	public static void setCounters(String strGUID, JSONArray jaNewCounterSet) {
		
		CollectorManager.hmCountersBean.put(strGUID, jaNewCounterSet);
	}
	
	/**
	 * @param hmCountersBean the hmCountersBean to set
	 */
	public static void setSlaCounters(String strGUID, JSONArray jaNewCounterSet) {
		
		CollectorManager.hmSlaCountersBean.put(strGUID, jaNewCounterSet);
	}
	/**
	 * to remove the counter set from hashmap table
	 * @param strGUID
	 */
	public static void removeCounterSet(String strGUID) {
		if(CollectorManager.hmCountersBean.containsKey(strGUID)) {
			CollectorManager.hmCountersBean.remove(strGUID);
		}
		
	}

	/**
	 * to remove the counter set from hashmap table
	 * @param strGUID
	 */
	public static void removeSlaCounterSet(String strGUID) {
		if(CollectorManager.hmSlaCountersBean.containsKey(strGUID)) {
			CollectorManager.hmSlaCountersBean.remove(strGUID);
		}
		
	}
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	private CollectorManager() {
		
		try{
			// initialize all required queue in this private constructor.
			pqModulePerformanceCounters = new PriorityBlockingQueue<CollectorBean>();
			pqModulePerformanceCountersV1 = new PriorityBlockingQueue<CollectorBean>();
			pqUnifiedAgentCounters = new PriorityBlockingQueue<UnifiedCounterDataBean>();
			pqNofificationData = new PriorityBlockingQueue<NotificationBean>();
			pqNetStackData = new PriorityBlockingQueue<NetStackDataBean>();
			pqJStackData = new PriorityBlockingQueue<JStackDataBean>();
			
			pqJavaProfiler = new PriorityBlockingQueue<String>();
			pqDotNetProfiler = new PriorityBlockingQueue<String>();
			pqPGSlowQueries = new PriorityBlockingQueue<String>();
			mssqlSlowQueries = new PriorityBlockingQueue<String>();
			mssqlProcedures = new PriorityBlockingQueue<String>();
			mysqlSlowQueries = new PriorityBlockingQueue<String>();
			oracleSlowQueries = new PriorityBlockingQueue<String>();
			beatIds = new PriorityBlockingQueue<String>();
			
			hmLatestCounters = new HashMap<String, Object[]>();
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	public void sendModuleService(byte[] dataBytes, String GUID, long installed_app_on, String command, String fileName) throws Exception {
		PostMethod method = null;
		HttpClient client = null;
		
		String databytes;
		Long LInstalled_app_on;
		
		try {
			databytes = new String(dataBytes);
			LInstalled_app_on = new Long(installed_app_on);
			method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/CreateAndStoreFileForOnPream");
			
			method.setParameter("dataBytes", databytes);
			method.setParameter("GUID", GUID);
			method.setParameter("installed_app_on", LInstalled_app_on.toString());
			method.setParameter("command", command);
			method.setParameter("fileName", fileName);
			
			client = new HttpClient();
			int statusCode = client.executeMethod(method);
			
			System.out.println("statusCode: "+ statusCode);
			
			method.setRequestHeader("Connection", "close");
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			
			
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
			LogManager.errorLog(e);
		}
		
	}
	
	public void WriteAndUploadFileOnFileCompare(Connection con, byte[] dataBytes, String GUID, long installed_app_on, String fileName) throws Exception {
		CollectorDBI collectorDBI = null;
		JSONObject joAWSKeys = null;
		String uploadFileName = "", uploadFilePath;
		OutputStream output = null;
		InputStream is = null;
		BufferedReader bf = null;
		try {
			
			collectorDBI = new CollectorDBI();
			
			long Uid = collectorDBI.getModuleId(con, GUID);
			
			uploadFileName = getAWSUploadFileNameOnFileCompare(Uid,fileName);
						
			String File_Path = Constants.APP_LIST_FILE_PATH+uploadFileName+".txt";
			//String File_Path = "F:\\mnt\\appedo\\"+uploadFileName+".txt";
			
			output = new FileOutputStream(new File(File_Path));
			
			PrintWriter pw = new PrintWriter(output);
			is = new ByteArrayInputStream(dataBytes);
			bf = new BufferedReader(new InputStreamReader(is));
			String app_list = null;
			while((app_list = bf.readLine()) != null) {
				//app_list.trim().replace("@", "");	
				pw.println(app_list.replaceAll("[@()=]", ""));
			}
			
			pw.close();
			
			//Upload File to Amazon S3
			
			AmazonS3APIWrapper s3Client = new AmazonS3APIWrapper();
			
			joAWSKeys = collectorDBI.getAmazonSecurityKeys(con);
			
			//upload file gets from constants
			uploadFilePath ="/mnt/appedo/compareFile/"+uploadFileName+".txt";
			//uploadFilePath ="F:\\mnt\\appedo\\"+uploadFileName+".txt";
			
			
			if(!joAWSKeys.isNullObject()) {
				s3Client.createAWSCredential(joAWSKeys.getString("access_key"), joAWSKeys.getString("secret_access_key"));
				
				s3Client.uploadFileInAws("apd-sys-configs", uploadFileName, uploadFilePath);
				
				collectorDBI.insertFileTransationDataOnFileCompare(con, Uid, "apd-sys-configs", uploadFileName, fileName, installed_app_on);
			}
		} catch (Exception e) {
			throw e;
		}
	}
	
	public void WriteAndUploadFileOnInstallAppList(Connection con, byte[] dataBytes, String GUID, long installed_app_on) throws Exception {
		CollectorDBI collectorDBI = null;
		JSONObject joAWSKeys = null;
		String uploadFileName = "", uploadFilePath, responseUploadUrl = null;
		OutputStream output = null;
		InputStream is = null;
		BufferedReader bf = null;
		try {
			
			collectorDBI = new CollectorDBI();
			
			long Uid = collectorDBI.getModuleId(con, GUID);
			
			uploadFileName = getAWSUploadFileName(Uid);
			
			//install list of data write in File
			/**
			 * File Writing location path getting from constants and adding fileName also
			 */
			
			//output = new FileOutputStream(new File("F:\\mnt\\appedo\\FileComparison\\"+uploadFileName+".txt"));
			//output = new FileOutputStream(new File("/mnt/appedo/compareFile/"+uploadFileName+".txt"));
			
			String File_Path = Constants.APP_LIST_FILE_PATH+uploadFileName+".txt";
			
			output = new FileOutputStream(new File(File_Path));
			
			//output.write(dataBytes);
			
			//output.close();
			
			PrintWriter pw = new PrintWriter(output);
			is = new ByteArrayInputStream(dataBytes);
			bf = new BufferedReader(new InputStreamReader(is));
			String app_list = null;
			while((app_list = bf.readLine()) != null) {
				//app_list.trim().replace("@", "");	
				pw.println(app_list.replaceAll("[@()=]", ""));
			}
			
			pw.close();
			
			
			//Upload File to Amazon S3
			
			AmazonS3APIWrapper s3Client = new AmazonS3APIWrapper();
			
			joAWSKeys = collectorDBI.getAmazonSecurityKeys(con);
			
			//upload file gets from constants
			uploadFilePath ="/mnt/appedo/compareFile/"+uploadFileName+".txt";
			
			if(!joAWSKeys.isNullObject()) {
				s3Client.createAWSCredential(joAWSKeys.getString("access_key"), joAWSKeys.getString("secret_access_key"));
				
				s3Client.uploadFileInAws("apd-sys-configs", uploadFileName, uploadFilePath);
				
				//responseUploadUrl = s3Client.getUploadedUrl("apd-sys-configs", uploadFileName);
				
				//System.out.println("uploadedUrl : "+ responseUploadUrl);
				//responseUploadUrl = "www.amazonS3.com";
				
				collectorDBI.insertFileTransationData(con, Uid, "apd-sys-configs", uploadFileName, responseUploadUrl, installed_app_on);
			}
			
			/*if(!responseUploadUrl.isEmpty()) {
				collectorDBI.insertFileTransationData(con, Uid, "apd-sys-configs", uploadFileName, responseUploadUrl);
			}*/
		} catch (Exception e) {
			throw e;
		}
	}
	
	public String getAWSUploadFileName (long uid) {
		
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		Date dateobj = new Date();
		
		String fileName = "Installed_list_"+uid+"_"+df.format(dateobj);
		
		return fileName;
	}
	
	public String getAWSUploadFileNameOnFileCompare (long uid, String strFileName) {
		
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		Date dateobj = new Date();
		
		String fileName = strFileName+"_"+uid+"_"+df.format(dateobj);
		
		return fileName;
	}
	
	/**
	 * Access the only[singleton] CollectorManager object.
	 * 
	 * @return CollectorManager
	 */
	public static CollectorManager getCollectorManager(){
		return collectorManager;
	}
	
	/*
	public StringBuilder initalizeAgentTransaction(String strGUID, String StrAgentType) throws Exception {
		Connection con = null;
		
		long lLastThreadId = 0l;
		String strEncryptedUID = null;
		StringBuilder sbReturn = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			DataBaseManager.doConnectionSetupIfRequired( InitServlet.CONSTANTS_PATH );
			con = DataBaseManager.giveConnection();
			
			CollectorDBI collectorDBI = new CollectorDBI();
			strEncryptedUID = collectorDBI.initalizeAgentTransaction(con, strGUID, StrAgentType);
			
			if( strEncryptedUID != null ){
				if( StrAgentType.equals("PROFILER") ){
					lLastThreadId = collectorDBI.getLastThreadId(con, strGUID);
				}
				
				hmKeyValues = new HashMap<String, Object>();
				hmKeyValues.put("uid", strEncryptedUID);
				hmKeyValues.put("lastThreadId", new Long(lLastThreadId));
				
				sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
			}
		} catch (Exception ex) {
			System.out.println("Exception in initalizeAgentTransaction: "+ex.getMessage());
			ex.printStackTrace();
			throw ex;
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		return sbReturn;
	}
	*/
	
	/** Commented as UID-Concept is removed
	 * 
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 *
	public StringBuilder getModuleConfigurations(String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		String strEncryptedUID = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			strEncryptedUID = collectorDBI.getEncryptedModuleUID(con, strGUID);
			
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("uid", strEncryptedUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			throw ex;
		}
		
		strEncryptedUID = null;
		UtilsFactory.clearCollectionHieracy(hmKeyValues);
		hmKeyValues = null;
		
		return sbReturn;
	}*/
	
	/**
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 *
	public StringBuilder getApplicationConfigurations(String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		String strEncryptedUID = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			strEncryptedUID = collectorDBI.getEncryptedApplicationUID(con, strGUID);
			
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("uid", strEncryptedUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			System.out.println("Exception in getApplicationUID: "+ex.getMessage());
			throw ex;
		}
		
		return sbReturn;
	}
	
	/**
	 * Returns the server agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 *
	public StringBuilder getServerConfigurations(String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		String strEncryptedUID = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			strEncryptedUID = collectorDBI.getEncryptedServerUID(con, strGUID);
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("uid", strEncryptedUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			System.out.println("Exception in getServerUID: "+ex.getMessage());
			throw ex;
		}
		
		return sbReturn;
	}
	
	/**
	 * Returns the database agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 *
	public StringBuilder getDatabaseConfigurations(String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		String strEncryptedUID = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			strEncryptedUID = collectorDBI.getEncryptedDatabaseUID(con, strGUID);
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("uid", strEncryptedUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			System.out.println("Exception in getDatabaseUID: "+ex.getMessage());
			throw ex;
		}
		
		return sbReturn;
	}
	
	
	public StringBuilder getNetworkConfigurations(String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		String strEncryptedUID = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			strEncryptedUID = collectorDBI.getEncryptedNetworkUID(con, strGUID);
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("uid", strEncryptedUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			System.out.println("Exception in getNetworkUID: "+ex.getMessage());
			throw ex;
		}
		
		return sbReturn;
	}*/
	
	/**
	 * Returns the application profiler agent's configuration details such as UID & last ThreadId used for this GUID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public StringBuilder getProfilerConfigurations(Connection con, String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		long lLastThreadId = 0l, lUID = 0l;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			
			lUID = collectorDBI.getApplicationUID(con, strGUID);
			
			JavaProfilerDBI javaProfilerDBI = new JavaProfilerDBI();
			lLastThreadId = javaProfilerDBI.getLastThreadId(con, lUID);
			
			hmKeyValues = new HashMap<String, Object>();
			// Commented as UID-Concept is removed
			// hmKeyValues.put("uid", strEncryptedUID);
			
			hmKeyValues.put("lastThreadId", new Long(lLastThreadId));
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			throw ex;
		} finally {
			//strEncryptedUID = null;
			lLastThreadId = 0l;
			lUID = 0l;
			UtilsFactory.clearCollectionHieracy(hmKeyValues);
			hmKeyValues = null;
		}
		
		return sbReturn;
	}
	
	/**
	 * TODO enable_backup
	 *
	public void backupTables(){
		
		try{
			//System.out.println("Starting DBBackup timer thread(db insert): "+(new Date()));
			
			CollectorDBI collectorDBI = new CollectorDBI();
			collectorDBI.backupPreviousDayRecords(conBackup);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}*/
	
	/**
	 * Queue the given Counter CollectorBean into the asked Family queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */
	public boolean collectPerformanceCounters(String strCounterParams) throws Exception {
		CollectorBean collBean = null;
		
		try {
			collBean = new CollectorBean();
			collBean.setCounterParams(strCounterParams);
			collBean.setReceivedOn(new Date());
			
			return pqModulePerformanceCounters.add(collBean);
		} catch(Exception e) {
			throw e;
		}
	}
	/**
	 * Poll the top Counter CollectorBean from the asked Family queue.
	 * 
	 * @param agent_family
	 * @param nIndex
	 * @return
	 */
	public CollectorBean pollCounter(){
		CollectorBean cbCounterEntry = null;
		
		try {
			cbCounterEntry = pqModulePerformanceCounters.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return cbCounterEntry;
	}
	/**
	 * Get the size of the asked Family queue.
	 * 
	 * @return int
	 */
	public int getCounterLength(){
		return pqModulePerformanceCounters.size();
	}

	/**
	 * Queue the given Counter CollectorBean into the asked Family queue.
	 * 
	 * @param joCounterParams
	 * @return
	 * @throws Exception
	 */
	public boolean collectPerformanceJSONCounters(JSONObject joCounterParams, String strModuleName, String strUUID) throws Exception {
		CollectorBean collBean = null;
		
		try {
			collBean = new CollectorBean();
			collBean.setJSONCounterParams(joCounterParams.getJSONObject("counterSet"));
			collBean.setStrModuleName(strModuleName);
			if(joCounterParams.containsKey("version")){
				collBean.setStrModuleVersion(joCounterParams.getString("version").toString());
			}
			//collBean.setStrModuleVersion(joCounterParams.getString("version").toString());
			if(joCounterParams.containsKey("eid")){
				collBean.setStrEnterpriseId(joCounterParams.getString("eid").toString());
			}
			//collBean.setStrEnterpriseId(joCounterParams.getString("eid").toString());
			collBean.setStrUUID(strUUID);
			if(joCounterParams.containsKey("oldGuid")){
				collBean.setStrOldGUID(joCounterParams.getString("oldGuid"));
			}
			//collBean.setStrOldGUID(joCounterParams.getString("oldGuid"));
			//collBean.setCounterParams(strCounterParams);
			collBean.setReceivedOn(new Date());
			
			return pqModulePerformanceCountersV1.add(collBean);
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Poll the top Counter CollectorBean from the asked Family queue.
	 * 
	 * @param agent_family
	 * @param nIndex
	 * @return
	 */
	public CollectorBean pollJSONCounter(){
		CollectorBean cbCounterEntry = null;
		
		try {
			cbCounterEntry = pqModulePerformanceCountersV1.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return cbCounterEntry;
	}
	
	/**
	 * Queue the given Counter CollectorBean into the asked Family queue.
	 * 
	 * @param joCounterData
	 * @return
	 * @throws Exception
	 */
	public boolean collectUnifiedAgentCounters(JSONObject joCounterData) throws Exception {
		//CollectorBean collBean = null;
		UnifiedCounterDataBean uaCounterBean = null;
		
		try {
			uaCounterBean = new UnifiedCounterDataBean();
			uaCounterBean.setCounterData(joCounterData);
			uaCounterBean.setDateQueuedOn(new Date().getTime());
			
			return pqUnifiedAgentCounters.add(uaCounterBean);
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Get the size of the asked Family queue.
	 * 
	 * @return int
	 */
	public int getUnifiedAgentCounterSize(){
		return pqUnifiedAgentCounters.size();
	}
	
	/**
	 * Get all data from the queue.
	 * 
	 * @param agent_family
	 * @param nIndex
	 * @return
	 */
	public ArrayList<UnifiedCounterDataBean> pollUACounter(){
		ArrayList<UnifiedCounterDataBean> alCounterEntries = new ArrayList<UnifiedCounterDataBean>();
		
		try {
			pqUnifiedAgentCounters.drainTo(alCounterEntries);
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return alCounterEntries;
	}
	
	/**
	 * Queue the given Counter CollectorBean into the asked Family queue.
	 * 
	 * @param joNetStackData
	 * @return
	 * @throws Exception
	 */
	public boolean collectNetStackData(JSONObject joNetStackData) throws Exception {
		NetStackDataBean netStackDataBean = null;
		
		try {
			netStackDataBean = new NetStackDataBean();
			netStackDataBean.setNetStackData(joNetStackData);
			netStackDataBean.setDateQueuedOn(new Date().getTime());
			
			return pqNetStackData.add(netStackDataBean);
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Get the size of the asked Family queue.
	 * 
	 * @return int
	 */
	public int getNetStackDataSize(){
		return pqNetStackData.size();
	}
	
	/**
	 * Get all data from the queue.
	 * 
	 * @param agent_family
	 * @param nIndex
	 * @return
	 */
	public ArrayList<NetStackDataBean> pollNetStackData(){
		ArrayList<NetStackDataBean> alNetStackEntries = new ArrayList<NetStackDataBean>();
		
		try {
			pqNetStackData.drainTo(alNetStackEntries);
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return alNetStackEntries;
	}
	
	public boolean collectJStackData(JSONObject joJStackData) throws Exception {
		JStackDataBean jStackDataBean = null;
		
		try {
			jStackDataBean = new JStackDataBean();
			jStackDataBean.setJStackData(joJStackData);
			jStackDataBean.setDateQueuedOn(new Date().getTime());
			
			return pqJStackData.add(jStackDataBean);
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Get the size of the asked Family queue.
	 * 
	 * @return int
	 */
	public int getJStackDataSize(){
		return pqJStackData.size();
	}
	
	/**
	 * Get all data from the queue.
	 * 
	 * @param agent_family
	 * @param nIndex
	 * @return
	 */
	public ArrayList<JStackDataBean> pollJStackData(){
		ArrayList<JStackDataBean> alJStackEntries = new ArrayList<JStackDataBean>();
		
		try {
			pqJStackData.drainTo(alJStackEntries);
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return alJStackEntries;
	}
	
	/**
	 * Queue the given Counter CollectorBean into the asked Family queue.
	 * 
	 * @param joCounterData
	 * @return
	 * @throws Exception
	 */
	public boolean collectNotificationData(JSONObject joCounterData) throws Exception {
		//CollectorBean collBean = null;
		NotificationBean notificationBean = null;
		
		try {
			notificationBean = new NotificationBean();
			notificationBean.setNotificationData(joCounterData);
			notificationBean.setDateQueuedOn(new Date().getTime());
			
			return pqNofificationData.add(notificationBean);
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Get the size of the asked Family queue.
	 * 
	 * @return int
	 */
	public int getNotificationDataSize(){
		return pqNofificationData.size();
	}
	
	/**
	 * Get all data from the queue.
	 * 
	 * @param agent_family
	 * @param nIndex
	 * @return
	 */
	public ArrayList<NotificationBean> pollNotificationData(){
		ArrayList<NotificationBean> alNotificationData = new ArrayList<NotificationBean>();
		
		try {
			pqNofificationData.drainTo(alNotificationData);
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return alNotificationData;
	}
	
	/**
	 * Collect and add the Java's profiled data into MySQL queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */
	public boolean collectJavaProfiler(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return pqJavaProfiler.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	/**
	 * Get the top counter data from the java profile data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollJavaProfiler(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = pqJavaProfiler.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	/**
	 * Get the size of Java profile entries queue.
	 * 
	 * @return int
	 */
	public int getJavaProfilerLength(){
		return pqJavaProfiler.size();
	}
	
	/**
	 * Collect and add the DOTNET's profiled data into MySQL queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */
	public boolean collectDotNetProfiler(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return pqDotNetProfiler.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	/**
	 * Collect and add the POSTGRES's slow queries data into postgres queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */

	public boolean collectPGSlowQueries(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return pqPGSlowQueries.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	/**
	 * Collect and add the MSSQL's slow queries data into MSSQL queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */

	public boolean collectMSSQLSlowQueries(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return mssqlSlowQueries.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	/**
	 * Collect and add the MSSQL's procedures data into MSSQL queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */
	
	
	public boolean collectMSSQLProcedures(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return mssqlProcedures.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Collect and add the MSSQL's slow queries data into MSSQL queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */
	
	public boolean collectMySQLSlowQueries(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return mysqlSlowQueries.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * Collect and add the Oracle's slow queries data into Oracle queue.
	 * 
	 * @param strCounterParams
	 * @return
	 * @throws Exception
	 */
	public boolean collectOracleSlowQueries(String GUID, String strCounterParams) throws Exception {
		
		try {
			if( strCounterParams != null ) {
				return oracleSlowQueries.add(strCounterParams);
			} else {
				LogManager.errorLog("Null/blank message came from GUID: "+GUID);
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	/**
	 * Get the top counter data from the POSTGRES data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollPGSlowQueries(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = pqPGSlowQueries.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	/**
	 * Get the top counter data from the MYSQL data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollMssqlSlowQueries(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = mssqlSlowQueries.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	/**
	 * Get the top counter data from the MSSQL data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollMssqlProcedures(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = mssqlProcedures.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	/**
	 * Get the top counter data from the DOTNET profile data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollDotNetProfiler(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = pqDotNetProfiler.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	
	/**
	 * Get the top counter data from the MYSQL data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollMysqlSlowQueries(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = mysqlSlowQueries.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	
	/**
	 * Get the slow queries data from the Oracle data queue.
	 * 
	 * @return Counter JSON data in String format
	 */
	public String pollOracleSlowQueries(){
		String strCounterEntry = null;
		
		try {
			strCounterEntry = oracleSlowQueries.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	
	/**
	 * Get the size of Postgres Slow Queries entries queue.
	 * 
	 * @return int
	 */
	public int getPGSlowQueriesLength(){
		return pqPGSlowQueries.size();
		
	}
	/**
	 * Get the size of MSSQL Slow Queries entries queue.
	 * 
	 * @return int
	 */
	public int getMSSQLSlowQueriesLength(){
		return mssqlSlowQueries.size();
		
	}
	/**
	 * Get the size of MSSQL procedures entries queue.
	 * 
	 * @return int
	 */
	public int getMSSQLProcedureLength(){
		return mssqlProcedures.size();
		
	}
	/**
	 * Get the size of DOTNET profile entries queue.
	 * 
	 * @return int
	 */
	public int getDotNetProfilerLength(){
		return pqDotNetProfiler.size();
	}
	
	/**
	 * Get the size of MYSQL Slow Queries entries queue.
	 * 
	 * @return int
	 */
	public int getMySQLSlowQueriesLength(){
		return mysqlSlowQueries.size();
		
	}
	
	/**
	 * Get the size of Postgres Slow Queries entries queue.
	 * 
	 * @return int
	 */
	public int getOracleSlowQueriesLength(){
		return oracleSlowQueries.size();
		
	}
	
	/**
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Object> getModuleConfigCountersTest(Connection con, String strUUID) throws Exception {
		HashMap<String, Object> hmKeyValues = new HashMap<String, Object>();
		HashMap<String, Object> hmResponse = new HashMap<String, Object>();
		JSONArray jaCounters = null;
		JSONObject joLicense = null;
	
		JSONObject joGUID = null;
		JSONArray jaSlaCounters = null;
		JSONObject joVal = null, joSystemData = null;
		String strResponseMsg;
		boolean bValidLicense;
		try{
			//String[] moduleNames = {"MSIIS","MSSQL","Windows"};
			CollectorDBI collectorDBI = new CollectorDBI(); 
			// TODO get the new Counter-set to be collected
			
			joSystemData = new JSONObject();
			
			//long lSystemId = collectorDBI.getSystemId(con, strUUID);
			joSystemData = collectorDBI.getSystemDetails(con, strUUID);
						
			if( joSystemData.getLong("SystemId") == -1l ) {
				hmKeyValues.put("message", "System information not available, Contact administrator.");
			} else {
				joLicense = new JSONObject();
				joGUID = new JSONObject();
				joGUID.put("guid", "");
				//joGUID.put("message", "");
				hmKeyValues.put("WINDOWS", joGUID);
				hmKeyValues.put("MSSQL", joGUID);
				hmKeyValues.put("MSIIS", joGUID);
				
				hmKeyValues = collectorDBI.getModuleType(con, strUUID, joSystemData.getLong("SystemId"), hmKeyValues);
				
				//License Details for particular user
				joLicense = collectorDBI.getAPMUserLicenseDetails(con, joSystemData.getLong("userId"));
				hmResponse = hmKeyValues;
				
				for (Map.Entry<String, Object> key: hmKeyValues.entrySet()){
					joVal = JSONObject.fromObject(key.getValue());
					if(joVal.containsKey("uid")){
						
						// get user selected counter for monitor
						if( joLicense != null){
							jaSlaCounters = new JSONArray();
							
							// get metric counterset.
							jaCounters = collectorDBI.getConfigCounters(con, joVal.getLong("uid"), joLicense);
							joVal.put("counters", jaCounters);
							
							// get user mapped counters for sla
							jaSlaCounters = getSlaConfigCountersV2(joVal.getString("guid"));
							joVal.put("SLA", jaSlaCounters);

							hmResponse.put(key.getKey(), joVal);
						}
					}else{
						bValidLicense = validateLicense(con, joSystemData.getLong("userId"), joLicense);
						if(!bValidLicense){
							strResponseMsg = agentProgressStatus(con, strUUID, key.getKey());
							joVal.put("message", strResponseMsg);
							hmKeyValues.put(key.getKey(), joVal);
						}else{
							//send License Exceeded Response message.
							joVal.put("message", "License Exceeded contact administrator");
							hmKeyValues.put(key.getKey(), joVal);
						}
					}
				}

			}
		} catch (Exception ex) {
			if( ex.getStackTrace()[0].getMethodName().equals("validateLicense") ) {
				LogManager.errorLog("NullPointerException for user: "+joSystemData.toString());
			}
			throw ex;
		} finally{
			joLicense = null;
		}
		
		return hmKeyValues;
	}

	public String getProfilerModuleConfigGuid(Connection con, String strUUID, String strEid) throws Exception {
		
		JSONObject joLicense = null;

		JSONObject  joSystemData = null;
		String strResponse = "";
		boolean bValidLicense;
		String strGuid ;
		try{
			
			CollectorDBI collectorDBI = new CollectorDBI(); 
						
			joSystemData = new JSONObject();
			
			//long lSystemId = collectorDBI.getSystemId(con, strUUID);
			joSystemData = collectorDBI.getSystemDetails(con, strUUID);
			
			if( joSystemData.getLong("SystemId") == -1l ) {
				strResponse = "System information not available, Contact administrator.";
			}else{
				
				strGuid = collectorDBI.getGUID(con, strUUID, joSystemData.getLong("SystemId"));
				
				if(strGuid.equals("")){
					
					joLicense = new JSONObject();
					joLicense = collectorDBI.getAPMUserLicenseDetails(con, joSystemData.getLong("userId"));
					bValidLicense = validateLicense(con, joSystemData.getLong("userId"), joLicense);
					if(bValidLicense){
						strResponse = "License Exceeded contact administrator";
					}else{
						strResponse = collectorDBI.addModules(con, strUUID, joSystemData, strEid);
					}
				}else{
					strResponse = strGuid;
				}
				
			}
			
		} catch (Exception ex) {
			throw ex;
		} finally {
			joLicense = null;
		}
		
		return strResponse;
	}
	
	/***
	 * 
	 * return Response message
	 * @param UUID
	 * @throws Exception
	 */
	public String agentProgressStatus(Connection con, String UUID, String strModuleName) throws Exception{
		JSONObject joAgentData = null;
		String strResponseMsg = "";
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			joAgentData = collectorDBI.agentProgressStatus(con, UUID, strModuleName);
			if(joAgentData != null){
				if(joAgentData.getBoolean("InProgress")){
					if(joAgentData.getString("Message").length() > 0){
						strResponseMsg = joAgentData.getString("Message");
						collectorDBI.DeleteAgentProgressJson(con, UUID, strModuleName);
					}else{
						strResponseMsg = "Inprogress";
					}
				}
			}
		} catch(Exception ex) {
			throw ex;
		}
		
		return strResponseMsg;
	}
	
	/***
	 * 
	 * Return true or false for validate license in particular user
	 * @param user_id
	 * @return
	 */
	public boolean validateLicense(Connection con, long lUser_Id, JSONObject joLicense) throws Exception{
		boolean isLimitExceeded = false;
		long lTotalUserModulesCount = 0;
		JSONObject joAPMLicenseDetails = joLicense;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI();
			//joAPMLicenseDetails = new JSONObject();

			//joAPMLicenseDetails = collectorDBI.getAPMUserWiseLicenseMonthWise(con, lUser_Id);
			
			lTotalUserModulesCount = collectorDBI.getUserAddedModulesCount(con, lUser_Id);
			
			if(joAPMLicenseDetails.getInt("apm_max_agents") <= lTotalUserModulesCount && joAPMLicenseDetails.getInt("apm_max_agents") != -1 ){
				// User max modules count exceeds
				isLimitExceeded = true;
			}
		} catch(Exception ex) {
			if( ex.getStackTrace()[0].getMethodName().equals("validateLicense") ) {
				LogManager.errorLog("NullPointerException for user: "+lUser_Id);
			}
			throw ex;
		} finally {
			joAPMLicenseDetails = null;
		}
		return isLimitExceeded;
	}
	
	/**
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Object> getModuleConfigCounters(Connection con, String strGUID) throws Exception {
		HashMap<String, Object> hmKeyValues = new HashMap<String, Object>();
		JSONArray jaCounters = null;
		JSONObject joLicense = null, joUserLic = null;
	
		JSONArray jaSlaCounters = null;
		
		try{
			CollectorDBI collectorDBI = new CollectorDBI(); 
			// TODO get the new Counter-set to be collected
			
			long lUID = collectorDBI.getModuleUID(con, strGUID);
			if( lUID == -1l ) {
				hmKeyValues.put("KILL", true);		
			} else {
				// get UserId and License level
				joUserLic = collectorDBI.getUserLicense(con, strGUID);
				
				// get APM License details
				joLicense = collectorDBI.getLicenseAPMDetails(con, joUserLic);
				
				// get user selected counter for monitor
				if( joLicense != null){
					jaCounters = collectorDBI.getConfigCounters(con, lUID, joLicense);
					
					hmKeyValues.put("MonitorCounterSet", jaCounters);
				}else {
					hmKeyValues.put("MonitorCounterSet", jaCounters);	
				}
				
				// get user mapped counters for sla
				jaSlaCounters = getSlaConfigCounters(strGUID);
				//System.out.println("jaSlaCounters " + jaSlaCounters.toString());
				if(jaSlaCounters!=null) {
					//System.out.println("INSIDE IF ");
					hmKeyValues.put("SlaCounterSet", jaSlaCounters);
				}else  {
					//System.out.println("INSIDE else ");
					jaSlaCounters = new JSONArray();
				}
				
				// remove GUID from master object
				CollectorManager.hmCountersBean.remove(strGUID);
			}
		} catch (Exception ex) {
			throw ex;
		} finally{
			joLicense = null;
			joUserLic = null;
		}
		
		return hmKeyValues;
	}
	
	public JSONArray getSlaConfigCounters(String strGUID) {
		PostMethod method = null;
		HttpClient client = null;
		
		String responseJSONStream = null;
		
		JSONObject joGUIDWiseSLAs = null;
		JSONArray jaSLAs = null;
		
		try {
			method = new PostMethod(Constants.APPEDO_SLA_COLLECTOR_URL);
			method.setParameter("guid", strGUID);
			method.setParameter("command", "AgentFirstRequest");
			client = new HttpClient();
			int statusCode = client.executeMethod(method);
			method.setRequestHeader("Connection", "close");
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			} else {
				responseJSONStream = method.getResponseBodyAsString();
				
				//System.out.println("Sla Counters :" +responseJSONStream);
				if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
					JSONObject joResp = JSONObject.fromObject(responseJSONStream);
					
					if( joResp.getBoolean("success") ){
						if(joResp.getString("message") != null && joResp.getString("message").startsWith("{") && joResp.getString("message").endsWith("}")) {
							joGUIDWiseSLAs = joResp.getJSONObject("message");
							
							jaSLAs = joGUIDWiseSLAs.getJSONArray(strGUID);
						}
					}
				}
			}
		} catch(Exception e) {			
			LogManager.errorLog(e);
		}
		
		return jaSLAs;
	}
	
	public JSONArray getSlaConfigCountersV2(String strGUID) {
		PostMethod method = null;
		HttpClient client = null;
		
		String responseJSONStream = null;
		
		JSONObject joGUIDWiseSLAs = null;
		JSONArray jaSLAs = null;
		
		try {
			method = new PostMethod(Constants.APPEDO_SLA_COLLECTOR_URL);
			method.setParameter("guid", strGUID);
			method.setParameter("command", "UnifiedAgentFirstRequest");
			client = new HttpClient();
			int statusCode = client.executeMethod(method);
			method.setRequestHeader("Connection", "close");
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			} else {
				responseJSONStream = method.getResponseBodyAsString();
				
				//System.out.println("Sla Counters :" +responseJSONStream);
				if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
					JSONObject joResp = JSONObject.fromObject(responseJSONStream);
					
					if( joResp.getBoolean("success") ){
						if(joResp.getString("message") != null && joResp.getString("message").startsWith("{") && joResp.getString("message").endsWith("}")) {
							joGUIDWiseSLAs = joResp.getJSONObject("message");
							
							jaSLAs = joGUIDWiseSLAs.getJSONArray(strGUID);
						}
					}
				}
			}
		} catch(Exception e) {			
			LogManager.errorLog(e);
		}
		
		return jaSLAs;
	}
	
	public StringBuilder getModuleStatus(Connection con, String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		HashMap<String, Object> hmKeyValues = null;
		JSONArray jaCounters = null, jaSlaCounters = null;
		JSONObject joModuleStatus = null, joModuleCounters = null;
		CollectorDBI collectorDBI = null;
		try{
			collectorDBI = new CollectorDBI();
			hmKeyValues = new HashMap<>();
			joModuleCounters = new JSONObject();
			joModuleStatus = new JSONObject();
			
			joModuleStatus = collectorDBI.getModuleStatus(con, strGUID);
			
			joModuleCounters.put("message", joModuleStatus.getString("user_status"));
			if(joModuleStatus.getString("user_status").equalsIgnoreCase("restart") && !joModuleStatus.getString("module_type").equalsIgnoreCase("NETSTACK")){
				jaSlaCounters = new JSONArray();
				jaCounters = collectorDBI.getConfigCountersV2(con, joModuleStatus.getLong("uid"));
				joModuleCounters.put("counters", jaCounters);
				//hmKeyValues.put("counters", jaCounters);
				
				// get user mapped counters for sla
				jaSlaCounters = getSlaConfigCountersV2(strGUID);
				joModuleCounters.put("SLA", jaSlaCounters);
				//hmKeyValues.put("SLA", jaCounters);
			}
			
			if(joModuleStatus.getString("user_status").equalsIgnoreCase("deleted")){
				hmKeyValues.put("message", joModuleStatus.getString("user_status"));
			}else{
				hmKeyValues.put(joModuleStatus.getString("module_type"), joModuleCounters);
			}
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
			
			if(joModuleStatus.getString("user_status").equalsIgnoreCase("restart")){
				collectorDBI.updateModuleStatus(con, strGUID);
			}
		} catch (Exception ex) {
			throw ex;
		}
		
		return sbReturn;
	}
	
	/**
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public StringBuilder getModuleConfigurations(String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		HashMap<String, Object> hmKeyValues = null;
		JSONArray jaCounters = null;
		
		try{
			// TODO get the new Counter-set to be collected
			jaCounters = CollectorManager.getCounters(strGUID);
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("MonitorCounterSet", jaCounters);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
			// remove guid from master
			
			CollectorManager.removeCounterSet(strGUID);
		} catch (Exception ex) {
			throw ex;
		}
		
		return sbReturn;
	}
	/**
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public StringBuilder setModuleConfigurations(String strGUID, String strnewCounterSet) throws Exception {
		StringBuilder sbReturn = null;
		
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			LogManager.infoLog("strnewCounterSet :"+strnewCounterSet);
			CollectorManager.setCounters( strGUID, JSONArray.fromObject(strnewCounterSet));
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("guid", strGUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			throw ex;
		}
		
		return sbReturn;
	}
	
	public StringBuilder queueNewMonitorCounterSet(Connection con, String strGUID) throws Exception {
		StringBuilder sbReturn = null;
		JSONArray jaCounters = null, jaSLACounters = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			collectorManager = CollectorManager.getCollectorManager();
			hmKeyValues = collectorManager.getModuleConfigCounters(con, strGUID);
			
			if( hmKeyValues.containsKey("KILL") ) {
				sbReturn = UtilsFactory.getJSONSuccessReturn("kill");
			} else if( hmKeyValues.size() > 0 ) {
				jaCounters = (JSONArray)hmKeyValues.get("MonitorCounterSet");
				CollectorManager.setCounters( strGUID, jaCounters);
				
				jaSLACounters = (JSONArray)hmKeyValues.get("SlaCounterSet");
				CollectorManager.setSlaCounters(strGUID, jaSLACounters);
			} else {
				sbReturn = UtilsFactory.getJSONFailureReturn("Unable to get UID. Try again later.");
			}
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			throw ex;
		}
		
		return sbReturn;
	}
	/**
	 * Returns the application agent's configuration details such as UID.
	 * 
	 * @param strGUID
	 * @return
	 * @throws Exception
	 */
	public StringBuilder setSlaModuleConfigurations(String strGUID, JSONArray jaGUIDNewSLAs) throws Exception {
		StringBuilder sbReturn = null;
		HashMap<String, Object> hmKeyValues = null;
		
		try{
			CollectorManager.setSlaCounters(strGUID, jaGUIDNewSLAs);
			
			hmKeyValues = new HashMap<String, Object>();
			hmKeyValues.put("guid", strGUID);
			
			sbReturn = UtilsFactory.getJSONSuccessReturn(hmKeyValues);
		} catch (Exception ex) {
			throw ex;
		}
		
		return sbReturn;
	}

	
	public  void updateStatusOfCounterMaster(Connection con, String strGUID) {
		CollectorDBI collectorDBI = null;
		
		try{
			collectorDBI = new CollectorDBI();
			collectorDBI.updateStatusOfCounterMaster(con, strGUID);
		} catch (Exception ex) {
			LogManager.errorLog(ex);
		} finally {
			collectorDBI = null;
		}
		
	}
	
	
	public StringBuilder updateChildCounters(Connection con, String strGUID, JSONObject joRecChildCounters) {
		StringBuilder sbReturn = null;
		CollectorDBI collectorDBI = null;
		
		JSONArray jaAry = new JSONArray();
		JSONObject joParentCounterDetails = new JSONObject(), joAppChildCounter = null, joChild = null;
		
		try{
			collectorDBI = new CollectorDBI();
			
			long lUID = collectorDBI.getModuleUID(con, strGUID);
			if( lUID == -1l ) {
				sbReturn = UtilsFactory.getJSONSuccessReturn("kill");
				//throw new Exception("Given GUID is not matching: "+strGUID);				
			} else {
				jaAry = joRecChildCounters.getJSONArray("parentcounter");
				for(int i=0; i<jaAry.size(); i++){
					joChild = jaAry.getJSONObject(i);
					
					int nParentCounterId = joChild.getInt("parentcounterid");
					
					// get the parent counter instance, category and counter-name
					joParentCounterDetails = collectorDBI.getParentCounterDetails(con, lUID, nParentCounterId );
					
					if( joParentCounterDetails != null ) {
						collectorDBI.updateParentCounter(con, joParentCounterDetails);				
					}
					
					JSONArray jaChild = joChild.getJSONArray("childcounterdetail");
					
					for(int j=0; j<jaChild.size(); j++) {
						JSONObject joChildDetails = jaChild.getJSONObject(j);
						String strInstanceName = joChildDetails.getString("instancename");
						String strCounterName = joChildDetails.getString("countername");
						String strQuery = joChildDetails.getString("query");
						String strCategory = joChildDetails.getString("category");
						
						// If CounterName-InstanceName is same as Parent's then,
						// it is the Parent Counter, which is reported in the Children list.
						// So, that can be ignored.
						if( ! strInstanceName.equalsIgnoreCase( joParentCounterDetails.getString("instance_name") ) 
							&& ! strCounterName.equalsIgnoreCase( joParentCounterDetails.getString("counter_name") ) 
						) {
							joAppChildCounter = new JSONObject();
							joAppChildCounter.put("guid", strGUID);
							joAppChildCounter.put("queryString", strQuery);
							joAppChildCounter.put("executionType", "");
							joAppChildCounter.put("category", strCategory);
							joAppChildCounter.put("instance_name", strInstanceName);
							joAppChildCounter.put("counterName", strCounterName);
							joAppChildCounter.put("is_selected", false);
							joAppChildCounter.put("is_enabled", true);
							joAppChildCounter.put("has_instance", false);
							
							// If InstanceName is suffixed in CounterName then,
							// append the InstanceName in child's CounterName.
							if( ! strCounterName.endsWith("-"+strInstanceName) ) {
								joAppChildCounter.put("display_name", strCounterName+"-"+strInstanceName);
							} else {
								joAppChildCounter.put("display_name", strCounterName);
							}
							
							collectorDBI.insertCounterMasterTable(con,joAppChildCounter,lUID,nParentCounterId);
						}
						
						UtilsFactory.clearCollectionHieracy(joAppChildCounter);
						UtilsFactory.clearCollectionHieracy(joChildDetails);
						joChildDetails = null;
						strInstanceName = null;
						strCounterName = null;
						strQuery = null;
					}
				}
				
				sbReturn = UtilsFactory.getJSONSuccessReturn("Updated");
			}
		} catch (Exception ex) {
			LogManager.errorLog(ex);
			sbReturn = UtilsFactory.getJSONFailureReturn("Unable to update child counters. Try again later.");
		} finally {
			UtilsFactory.clearCollectionHieracy(joParentCounterDetails);
			UtilsFactory.clearCollectionHieracy(jaAry);
			
			collectorDBI = null;
		}
		
		return sbReturn;
	}
	
	/**
	 * to collect the list of upgrade module agent's guids
	 * @param strGUID
	 * @param joUpgradeDetails
	 */
	public void collectUpgradeGUID(Connection con) {
		CollectorDBI collectDBI = null;
		ResultSet rst = null;
		JSONObject joUpgradeGUID = null;
		
		try {
			hmUpgradeAgentBean.clear();
			collectDBI = new CollectorDBI();
			rst = collectDBI.collectUpgradeGUID(con);
			while (rst.next()) {
				joUpgradeGUID = new JSONObject();
				joUpgradeGUID.put("guid", rst.getString("guid"));
				//joUpgradeGUID.put("download_url", rst.getString("download_url"));
				hmUpgradeAgentBean.put(rst.getString("guid"), joUpgradeGUID);				
			}
			System.out.println("hmUpgradeAgentBean : " + hmUpgradeAgentBean.size());
		}catch(Exception e) {
			LogManager.errorLog(e);
		}finally {
			if(rst!=null) {
				DataBaseManager.close(rst);
				rst = null;
			}			
			collectDBI = null; 
		}
	}
	
	public boolean collectBeatData(String strGUID) throws Exception {
		try {
			if( strGUID != null ) {
				return beatIds.add(strGUID);
			} else {
				LogManager.errorLog("Null/blank GUID");
				return false;
			}
		} catch(Exception e) {
			throw e;
		}
	}
	
	public String pollBeatData(){
		String strCounterEntry = null;
		try {
			strCounterEntry = beatIds.poll();
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
		
		return strCounterEntry;
	}
	
	public int getBeatDataLength(){
		return beatIds.size();
		
	}
	
	public JSONObject getSystemId(Connection con, String sysGeneratorUUID)throws Exception {
		CollectorDBI collectDBI = null;
		JSONObject serverInformation = null, ResponseMessage = null;
		
		try {
			
			collectDBI = new CollectorDBI();
			
			serverInformation = collectDBI.getServerInfo(con, sysGeneratorUUID);
			
			ResponseMessage = new JSONObject();
			
			if(serverInformation.getBoolean("isExistsUUID")) {
				ResponseMessage.put("message", "successfully getting System Id");
				ResponseMessage.put("systemId", serverInformation.getString("system_id"));
			}else {
				throw new Exception("System Id could not be found in server information tables .");
			}
		} catch (Exception ex) {
			throw ex;
		}
		
		return ResponseMessage;
	}
	
	public JSONObject collectServerData(Connection con, JSONObject sysInformation, String Encrypted_id, String VMWARE_Key)throws Exception {
		CollectorDBI collectDBI = null;
		JSONObject userInformation = null, serverInformation = null, ResponseMessage = null, serverSideUserInfo = null;
		String strUUID;
		long lSystemId;
		try {
			
			strUUID = (VMWARE_Key == null ? sysInformation.getString("UUID") : sysInformation.getString("UUID")+"-VM"+VMWARE_Key);
			
			collectDBI = new CollectorDBI();
			
			userInformation = collectDBI.getUserInfo(con, Encrypted_id, -1L);
			
			serverInformation = collectDBI.getServerInfo(con, strUUID);
			
			ResponseMessage = new JSONObject();
			
			if(serverInformation.getBoolean("isExistsUUID")) {
				
				if(!userInformation.getString("user_id").equals(serverInformation.getString("user_id"))) {
					//"This system already mapped with email_id@ please check with @name@"
					serverSideUserInfo = collectDBI.getUserInfo(con, "", serverInformation.getLong("user_id"));
					String message = "This system already mapped with "+serverSideUserInfo.getString("email_id")+" please check with "+serverSideUserInfo.getString("name");
					ResponseMessage.put("message", message);
				}else {
					//successfully update System information
					ResponseMessage.put("message", "successfully update System information");
				}
				
				ResponseMessage.put("systemId", serverInformation.getString("system_id"));
			}else {
				//insert ServerInformation in server_information Table
				lSystemId = collectDBI.insertServerInformation(con, sysInformation, userInformation.getLong("user_id"), strUUID);
				
				ResponseMessage.put("message", "successfully update System information");
				ResponseMessage.put("systemId", lSystemId);
			}
		} catch (Exception ex) {
			throw ex;
		}
		
		return ResponseMessage;
	}
	
	public JSONObject createServerModule(Connection con, JSONObject serverInformation, long systemId, String UUID, String EnterpriseId, JSONObject joCounterSet)throws Exception {
		
		PostMethod method = null;
		HttpClient client = null;
		String responseJSONStream = null;
		
		CollectorDBI collectorDBI = null;
		JSONObject ResponseMessage = null, joSystemData = null, joModuleData = null;
		
		//long moduleId = -1L;;
		
		//String[] splitName = serverInformation.getString("PRETTY_NAME").split(" ");
		String moduleTypeName = "", prettyName;
		
		try {
			
			//prettyName = splitName[0].toLowerCase();
			prettyName = serverInformation.getString("PRETTY_NAME").toLowerCase();
			
			if(prettyName.trim().toLowerCase().contains("amazon")) {
				moduleTypeName = "AMAZON_LINUX";
			}else if(prettyName.trim().toLowerCase().contains("centos")) {
				moduleTypeName = "CentOS";
			}else if(prettyName.trim().toLowerCase().contains("red hat")) {
				moduleTypeName = "RedHat";
			}else if(prettyName.trim().toLowerCase().contains("fedora")) {
				moduleTypeName = "Fedora";
			}else if(prettyName.trim().toLowerCase().contains("ubuntu")){
				moduleTypeName = "Ubuntu";
			}else if(prettyName.trim().toLowerCase().contains("solaris")){
				moduleTypeName = "Solaris";
			}
			collectorDBI = new CollectorDBI();
			joModuleData = collectorDBI.getModuleId(con, systemId, UUID, moduleTypeName, null, false);
			
			ResponseMessage = new JSONObject();
			
			if(!joModuleData.getBoolean("isExistsGUID")) {
				
				joSystemData = collectorDBI.getSystemData(con, UUID, moduleTypeName, "LINUX");
				
				method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/createModulesForJavaUnification");
				method.setParameter("moduleName", joSystemData.getString("moduleName"));
				method.setParameter("moduleTypeName", moduleTypeName);
				method.setParameter("moduleDescription", joSystemData.getString("moduleName"));
				method.setParameter("moduleVersion", serverInformation.getString("VERSION_ID"));
				method.setParameter("moduleType", "SERVER");
				//method.setParameter("clrVersion", "");
				method.setParameter("JocounterSet", joCounterSet.toString());
				method.setParameter("UUID", UUID);
				method.setParameter("systemId", joSystemData.getString("systemId"));
				method.setParameter("userId", joSystemData.getString("userId"));
				method.setParameter("e_id", EnterpriseId);
				
				
				client = new HttpClient();
				int statusCode = client.executeMethod(method);
				
				System.out.println("send serverInfo of statusCode: "+ statusCode);
				
				method.setRequestHeader("Connection", "close");
				if (statusCode != HttpStatus.SC_OK) {
					System.err.println("Method failed: " + method.getStatusLine());
				} else {
					responseJSONStream = method.getResponseBodyAsString();
					
					if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
						
						JSONObject joResp = JSONObject.fromObject(responseJSONStream);
						
						if( joResp.getBoolean("success") ){
							ResponseMessage.put("message", joResp.getString("message"));
							ResponseMessage.put("moduleGUID", joResp.getString("moduleGUID"));
							//ResponseMessage.put("isExistsGUID", joModuleData.getBoolean("isExistsGUID"));
							ResponseMessage.put("moduleTypeName", moduleTypeName);
						}else{
							ResponseMessage.put("errorMessage", joResp.getString("errorMessage"));
						}
					}
				}
				
			}else {
				//already moduel is created.
				//Server module successfully created.
				ResponseMessage.put("message", "successfully created Module.");
				ResponseMessage.put("moduleGUID", joModuleData.getString("moduleGUID"));
				//ResponseMessage.put("isExistsGUID", joModuleData.getBoolean("isExistsGUID"));
				ResponseMessage.put("moduleTypeName", moduleTypeName);
			}
		} catch(Exception ex) {
			throw ex;
		}
		return ResponseMessage;
	}
	
	/**
	 * 
	 * @param con
	 * @param moduleInformation
	 * @param systemId
	 * @param UUID
	 * @param EnterpriseId
	 * @param joCounterSet
	 * @return
	 * @throws Exception
	 */
	
	public JSONObject createApplicationModule(Connection con, JSONObject moduleInformation, long systemId, String UUID, String EnterpriseId, JSONObject joCounterSet)throws Exception {
		
		PostMethod method = null;
		HttpClient client = null;
		String responseJSONStream = null;
		
		CollectorDBI collectorDBI = null;
		JSONObject ResponseMessage = null, joSystemData = null, joModuleData = null;
		String jmxPort = null;
		try {
			
			ResponseMessage = new JSONObject();
			collectorDBI = new CollectorDBI();
			jmxPort = moduleInformation.containsKey("jmx_port")? moduleInformation.getString("jmx_port") : null;
			if(moduleInformation.getString("moduleTypeName").equalsIgnoreCase("jboss")) {
				joModuleData = collectorDBI.getModuleId(con, systemId, UUID, moduleInformation.getString("moduleTypeName"), jmxPort, false);
			}else {
				joModuleData = collectorDBI.getModuleId(con, systemId, UUID, moduleInformation.getString("moduleName"), jmxPort, true);
			}
			
			
			if(!joModuleData.getBoolean("isExistsGUID")) {
				joSystemData = collectorDBI.getServerInfo(con, UUID);
				
				method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/createModulesForJavaUnification");
				//method = new PostMethod("https://test.appedo.com/appedo_ui_module_services_new/apm/createModulesForJavaUnification");
				method.setParameter("moduleName", moduleInformation.getString("moduleName"));
				method.setParameter("moduleTypeName", moduleInformation.getString("moduleTypeName"));
				if (jmxPort == null) {
					method.setParameter("moduleDescription", moduleInformation.getString("moduleName"));
					//method.setParameter("JMXPort", jmxPort);
				} else {
					method.setParameter("moduleDescription", moduleInformation.getString("moduleName")+"::"+jmxPort);
					method.setParameter("JMXPort", jmxPort);
				}
				
				method.setParameter("moduleVersion", moduleInformation.getString("VERSION_ID"));
				method.setParameter("moduleType", "APPLICATION");
				//method.setParameter("JMXPort", jmxPort);
				//method.setParameter("clrVersion", "");
				method.setParameter("JocounterSet", joCounterSet.toString());
				method.setParameter("UUID", UUID);
				method.setParameter("systemId", systemId+"");
				method.setParameter("userId", joSystemData.getString("user_id"));
				method.setParameter("e_id", EnterpriseId);
				
				
				client = new HttpClient();
				int statusCode = client.executeMethod(method);
				
				System.out.println("send serverInfo of statusCode: "+ statusCode);
				
				method.setRequestHeader("Connection", "close");
				if (statusCode != HttpStatus.SC_OK) {
					System.err.println("Method failed: " + method.getStatusLine());
				} else {
					responseJSONStream = method.getResponseBodyAsString();
					
					if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
						
						JSONObject joResp = JSONObject.fromObject(responseJSONStream);
						
						if( joResp.getBoolean("success") ){
							ResponseMessage.put("message", joResp.getString("message"));
							ResponseMessage.put("moduleGUID", joResp.getString("moduleGUID"));
							ResponseMessage.put("moduleTypeName", moduleInformation.getString("moduleTypeName"));
						}else{
							ResponseMessage.put("errorMessage", joResp.getString("errorMessage"));
						}
					}
				}
			}else {
				ResponseMessage.put("message", "successfully created Module.");
				ResponseMessage.put("moduleGUID", joModuleData.getString("moduleGUID"));
				ResponseMessage.put("moduleTypeName", moduleInformation.getString("moduleTypeName"));
				if(moduleInformation.getString("moduleTypeName").equalsIgnoreCase("jboss")) {
					updateJbossInfo(con, joModuleData.getString("moduleGUID"), moduleInformation);
				}
			}
			
		} catch(Exception ex) {
			throw ex;
		}
		return ResponseMessage;
	}
	
	/**
	 * 
	 * @param con
	 * @param moduleInformation
	 * @param systemId
	 * @param UUID
	 * @param EnterpriseId
	 * @param joCounterSet
	 * @return
	 * @throws Exception
	 */
	
	public JSONObject createDataBaseModule(Connection con, JSONObject moduleInformation, long systemId, String UUID, String EnterpriseId, JSONObject joCounterSet)throws Exception {
		
		PostMethod method = null;
		HttpClient client = null;
		String responseJSONStream = null;
		
		CollectorDBI collectorDBI = null;
		JSONObject ResponseMessage = null, joSystemData = null, joModuleData = null;
		
		try {
			
			ResponseMessage = new JSONObject();
			collectorDBI = new CollectorDBI();
			joModuleData = collectorDBI.getModuleId(con, systemId, UUID, moduleInformation.getString("moduleName"), null, true);
			
			if(!joModuleData.getBoolean("isExistsGUID")) {
				joSystemData = collectorDBI.getServerInfo(con, UUID);
				
				method = new PostMethod(Constants.MODULE_UI_SERVICES+"/apm/createModulesForJavaUnification");
				//method = new PostMethod("https://test.appedo.com/appedo_ui_module_services_UnitTest/apm/createModulesForJavaUnification");
				method.setParameter("moduleName", moduleInformation.getString("moduleName"));
				method.setParameter("moduleTypeName", moduleInformation.getString("moduleTypeName"));
				method.setParameter("moduleDescription", moduleInformation.getString("moduleName"));
				method.setParameter("moduleVersion", moduleInformation.getString("VERSION_ID"));
				method.setParameter("moduleType", "DATABASE");
				//method.setParameter("clrVersion", "");
				method.setParameter("JocounterSet", joCounterSet.toString());
				method.setParameter("UUID", UUID);
				method.setParameter("systemId", systemId+"");
				method.setParameter("userId", joSystemData.getString("user_id"));
				method.setParameter("e_id", EnterpriseId);
				
				
				client = new HttpClient();
				int statusCode = client.executeMethod(method);
				
				System.out.println("send serverInfo of statusCode: "+ statusCode);
				
				method.setRequestHeader("Connection", "close");
				if (statusCode != HttpStatus.SC_OK) {
					System.err.println("Method failed: " + method.getStatusLine());
				} else {
					responseJSONStream = method.getResponseBodyAsString();
					
					if( responseJSONStream.startsWith("{") && responseJSONStream.endsWith("}")) {
						
						JSONObject joResp = JSONObject.fromObject(responseJSONStream);
						
						if( joResp.getBoolean("success") ){
							ResponseMessage.put("message", joResp.getString("message"));
							ResponseMessage.put("moduleGUID", joResp.getString("moduleGUID"));
							ResponseMessage.put("moduleTypeName", moduleInformation.getString("moduleTypeName"));
						}else{
							ResponseMessage.put("errorMessage", joResp.getString("errorMessage"));
						}
					}
				}
			}else {
				ResponseMessage.put("message", "successfully created Module.");
				ResponseMessage.put("moduleGUID", joModuleData.getString("moduleGUID"));
				ResponseMessage.put("moduleTypeName", moduleInformation.getString("moduleTypeName"));	
			}
			
		} catch(Exception ex) {
			throw ex;
		}
		return ResponseMessage;
	}
	
	public String moduleRunningStatus(Connection con, String GUID) throws Exception{
		
		CollectorDBI collectorDBI = null;
		String strModuleRunningStatus = "";
		
		try {
			collectorDBI = new CollectorDBI();
			strModuleRunningStatus = collectorDBI.getModuleRunningStatus(con, GUID);
			if(strModuleRunningStatus.equalsIgnoreCase("restart")) {
				//strModuleRunningStatus = "Running";
				collectorDBI.updateModuleStatus(con, GUID);
			}
		} catch (Exception ex) {
			throw ex;
		} finally {
			collectorDBI = null; 
		}
		
		return strModuleRunningStatus;
	}
	
	public void updateJbossInfo(Connection con, String GUID, JSONObject joJbossInfo) throws Exception{
		
		CollectorDBI collectorDBI = null;
		
		try {
			collectorDBI = new CollectorDBI();
			collectorDBI.updateJbossInfo(con, GUID, joJbossInfo);
		} catch (Exception ex) {
			throw ex;
		} finally {
			collectorDBI = null; 
		}
	}
	
	@Override
	/**
	 * Before destroying clear the objects. To prevent from MemoryLeak.
	 */
	protected void finalize() throws Throwable {
		UtilsFactory.clearCollectionHieracy(pqModulePerformanceCounters);
		UtilsFactory.clearCollectionHieracy(pqUnifiedAgentCounters);
		UtilsFactory.clearCollectionHieracy(pqNofificationData);
		UtilsFactory.clearCollectionHieracy(pqNetStackData);
		UtilsFactory.clearCollectionHieracy(pqJStackData);
		
		super.finalize();
	}
}
