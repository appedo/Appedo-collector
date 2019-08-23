package com.appedo.webcollector.webserver.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

import com.appedo.manager.AppedoConstants;
import com.appedo.manager.LogManager;

/**
 * This class holds the application level variables which required through the application.
 * 
 * @author Ramkumar
 *
 */
public class Constants {
	
	/**
	 * If "ENVIRONMENT" config-porperty is "DEVELOPMENT" in both DB & appedo_config.properties file then,
	 * it is Development/Testing Environment. So give priority to the properties available in appedo_config.properties file.
	 */
	public static boolean DEV_ENVIRONMENT = false;

	public static String RESOURCE_PATH = null;
	public static String CONSTANTS_FILE_PATH = null;
	public static String APPEDO_CONFIG_FILE_PATH = null;
	public static String SMTP_MAIL_CONFIG_FILE_PATH = null;
	
	// milliseconds unit
	public final static long FREQUENCY_ONE_DAY = 1000 * 60 * 60 * 24;
	public final static long FREQUENCY_ONE_HOUR = 1000 * 60 * 60;
	public final static long FREQUENCY_FOUR_HOUR = 1000 * 60 * 60 * 4;
	public final static long FREQUENCY_ONE_MINUTE = 1000 * 60 * 1;
	
	
	public static long COUNTER_CONTROLLER_REST_MILLESECONDS = 1000;
	public static long MODULE_INSERT_REST_MILLESECONDS = 20000;
	
	public static long DOTNET_PROFILER_COMPUTE_REST_MILLESECONDS = 1000;
	public static long DOTNET_PROFILER_INSERT_REST_MILLESECONDS = 1000;
	
	public static int MODULE_CONTROLLER_THREADS = 10;
	public static int UNIFIED_AGENT_COLLECTOR_THREADS = 10;
	public static int NOTIFICATION_DATA_COLLECTOR_THREADS = 10;
	public static int JAVA_PROFILER_CONTROLLER_THREADS = 10;
	public static int DOTNET_PROFILER_STACKTRACE_COMPUTE_THREADS = 10;
	public static int DOTNET_PROFILER_STACKTRACE_INSERT_THREADS = 10;
	public static int PG_PROFILER_CONTROLLER_THREADS = 10;
	public static int MSSQL_PROFILER_CONTROLLER_THREADS = 10;
	public static int MSSQL_PROCEDURE_CONTROLLER_THREADS = 10;
	public static int MYSQL_PROCEDURE_CONTROLLER_THREADS = 10;
	public static int ORACLE_PROFILER_CONTROLLER_THREADS = 10;
	public static final int BEATS_CONTROLLER_THREADS = 10;
	public static int UPGRADE_CHECK_INTERVAL = 60;

	public static long UNIFIED_AGENT_SERVICE_RUNTIME_INTERVAL_MS = 500;
	
	public static long UNIFIED_COUNTER_TIMER_CHECK = -1L;
	public static long NOTIFICATION_DATA_TIMER_CHECK = -1L;
	public static long UNIFIED_COUNTER_RUNNING_TIME = -1L;
	public static long NOTIFICATION_DATA_RUNNING_TIME = -1L;
	
	public static long MIN_ALLOWED_METHOD_EXECUTION_DURATION_MILLISEC = 0;
	public static int MAX_PROFILER_METHOD_TRACE_TO_COMPUTE = 200;
	
	public static String LOG4J_PROPERTIES_FILE;
	public static String APPEDO_SLA_COLLECTOR_URL = null;
	public static String MODULE_UI_SERVICES = null;

	public static String UNIFIED_AGENT_THREADPOOL_NAME = null;
	public static int UNIFIED_AGENT_MIN_THREAD = 0;
	public static int UNIFIED_AGENT_MAX_THREAD = 0;
	public static int UNIFIED_AGENT_QUEUE_SIZE = 0;

	public static long NET_STACK_AGENT_SERVICE_RUNTIME_INTERVAL_MS = 120000;	// 2 mins := 1000*60*2
	public static String NET_STACK_AGENT_THREADPOOL_NAME = null;
	public static int NET_STACK_AGENT_MIN_THREAD = 0;
	public static int NET_STACK_AGENT_MAX_THREAD = 0;
	public static int NET_STACK_AGENT_QUEUE_SIZE = 0;
	
	//Following constants added for JStack related changes
	public static long JAVA_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS = 250;
	public static String JAVA_STACK_DATA_SERVICE_THREADPOOL_NAME = null;
	public static int JAVA_STACK_DATA_SERVICE_MIN_THREAD = 0;
	public static int JAVA_STACK_DATA_SERVICE_MAX_THREAD = 0;
	public static int JAVA_STACK_DATA_SERVICE_QUEUE_SIZE = 0;
	
	//This is type in NetStack json raw data that comes to queue, if type changes this needs to be changed accordingly.
	public static String NET_STACK_MODULE = "NETSTACK";
	public static String JAVA_STACK_MODULE = "JSTACK";
	
	public static long NET_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS = 250;
	public static String NET_STACK_DATA_SERVICE_THREADPOOL_NAME = null;
	public static int NET_STACK_DATA_SERVICE_MIN_THREAD = 0;
	public static int NET_STACK_DATA_SERVICE_MAX_THREAD = 0;
	public static int NET_STACK_DATA_SERVICE_QUEUE_SIZE = 0;
	
	public static String APP_LIST_FILE_PATH = null;
	public static boolean IS_ONPREM = false;
	
	public enum AGENT_FAMILY {
		APPLICATION("APPLICATION"), 
		SERVER("SERVER"), 
		DATABASE("DATABASE"), 
		NETWORK("NETWORK");
		
		private String strAgentFamily;
		
		private AGENT_FAMILY(String agentFamily) {
			strAgentFamily = agentFamily;
		}
		
		public String getAgentFamily() {
			return this.strAgentFamily;
		}
		
		public String toString() {
			return strAgentFamily;
		}
	}
	
	public enum AGENT_TYPE {
		TOMCAT("TOMCAT", "APPLICATION"), JBOSS("JBOSS", "APPLICATION"), MSIIS("MSIIS", "APPLICATION"), 
		LINUX("LINUX", "SERVER"), WINDOWS("WINDOWS", "SERVER"), 
		MYSQL("MYSQL", "DATABASE"), MSSQL("MSSQL", "DATABASE"),
		POSTGRES("POSTGRES", "DATABASE"),ORACLE("ORACLE", "DATABASE"),
		NETWORK("NETWORK", "NETWORK"),
		JAVA_PROFILER("JAVA_PROFILER", "APPLICATION"), DOTNET_PROFILER("DOTNET_PROFILER", "APPLICATION");
		
		private String strAgentType;
		private String strAgentCategory;
		
		private AGENT_TYPE(String agentType, String agentCategory) {
			strAgentType = agentType;
			strAgentCategory = agentCategory;
		}

		public void setMySQLVersion(String strVersionNo) {
			strAgentType = "MYSQL "+strVersionNo;
		}
		
		public String getAgentCategory() {
			return this.strAgentCategory;
		}
		
		public String toString() {
			return strAgentType;
		}
	}
	
	public enum PROFILER_KEY {
		THREAD_ID(1), TYPE(2), START_TIME(3), DURATION_MS(4), APPROX_NANO_SEC_START(5), DURATION_NS(6), REQUEST_URI(7), LOCALHOST_NAME_IP(8), REFERER_URI(9), TICK_VALUE_MS(10),
		CLASS_NAME(51), METHOD_NAME(52), METHOD_SIGNATURE(53), CALLER_METHOD_ID(54), CALLEE_METHOD_ID(55), 
		QUERY(101), 
		EXCEPTION_TYPE(151), EXCEPTION_MESSAGE(152), EXCEPTION_STACKTRACE(153);
		
		private Integer key;
		
		private PROFILER_KEY(Integer nKey) {
			key = nKey;
		}
		
		public String toString() {
			return key+"";
		}
	}

	public enum SLOW_QUERY {
		QUERY("query"),CALLS("calls"),DURATION_MS("duration_ms"),STIME("stime");
		
		private String key;
		
		private SLOW_QUERY(String strKey) {
			key = strKey;
		}
		
		public String toString() {
			return key+"";
		}
	}
	/**
	 * Loads constants properties 
	 * 
	 * @param srtConstantsPath
	 */
	public static void loadConstantsProperties(String srtConstantsPath) {
		Properties prop = new Properties();
		InputStream is = null;
		
		try {
			is = new FileInputStream(srtConstantsPath);
			prop.load(is);
			
			// Appedo application's resource directory path
			RESOURCE_PATH = prop.getProperty("RESOURCE_PATH");
			
			APPEDO_CONFIG_FILE_PATH = RESOURCE_PATH+prop.getProperty("APPEDO_CONFIG_FILE_PATH");
			SMTP_MAIL_CONFIG_FILE_PATH = RESOURCE_PATH+prop.getProperty("SMTP_MAIL_CONFIG_FILE_PATH");
			
			COUNTER_CONTROLLER_REST_MILLESECONDS = Integer.parseInt(prop.getProperty("COUNTER_CONTROLLER_REST_MILLESECONDS"));
			
			UNIFIED_AGENT_THREADPOOL_NAME = prop.getProperty("UNIFIED_AGENT_THREADPOOL_NAME");
			
			NET_STACK_AGENT_THREADPOOL_NAME = prop.getProperty("NET_STACK_AGENT_THREADPOOL_NAME");
			
			JAVA_STACK_DATA_SERVICE_THREADPOOL_NAME = prop.getProperty("JAVA_STACK_DATA_SERVICE_THREADPOOL_NAME");
			
			NET_STACK_DATA_SERVICE_THREADPOOL_NAME = prop.getProperty("NET_STACK_DATA_SERVICE_THREADPOOL_NAME");
			
		} catch(Exception e) {
			LogManager.errorLog(e);
		} finally {
			prop.clear();
			
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Loads constants properties 
	 * 
	 * @param srtConstantsPath
	 */
	public static void loadAppedoConfigProperties(String srtConstantsPath) throws Throwable {
		Properties prop = new Properties();
		InputStream is = null;
		
		try {
			is = new FileInputStream(srtConstantsPath);
			prop.load(is);
			
			if( prop.getProperty("ENVIRONMENT") != null && prop.getProperty("ENVIRONMENT").equals("DEVELOPMENT") 
					&& AppedoConstants.getAppedoConfigProperty("ENVIRONMENT") != null && AppedoConstants.getAppedoConfigProperty("ENVIRONMENT").equals("DEVELOPMENT") ) 
			{
				DEV_ENVIRONMENT = true;
			}
			
			APPEDO_SLA_COLLECTOR_URL = getProperty("APPEDO_SLA_COLLECTOR", prop);
			
			MODULE_UI_SERVICES = getProperty("MODULE_UI_SERVICES", prop);
			
			IS_ONPREM = Boolean.parseBoolean( UtilsFactory.replaceNull(getProperty("IS_ONPREM", prop), "false") );
			
			MIN_ALLOWED_METHOD_EXECUTION_DURATION_MILLISEC = Integer.parseInt( UtilsFactory.replaceNull(getProperty("MIN_ALLOWED_METHOD_EXECUTION_DURATION_MILLISEC", prop), "0") );
			
			MAX_PROFILER_METHOD_TRACE_TO_COMPUTE = Integer.parseInt( UtilsFactory.replaceNull(getProperty("MAX_PROFILER_METHOD_TRACE_TO_COMPUTE", prop), "200") );
			
			// DOTNET_PROFILER_COMPUTE_REST_MILLESECONDS = Integer.parseInt(getProperty("DOTNET_PROFILER_COMPUTE_REST_MILLESECONDS"));
			// DOTNET_PROFILER_INSERT_REST_MILLESECONDS = Integer.parseInt(getProperty("DOTNET_PROFILER_INSERT_REST_MILLESECONDS"));
			
			//UNIFIED_AGENT_SERVICE_RUNTIME_INTERVAL_MS = Long.parseLong(getProperty("UNIFIED_AGENT_SERVICE_RUNTIME_INTERVAL_MS"));
			UNIFIED_AGENT_MIN_THREAD = Integer.parseInt(getProperty("UNIFIED_AGENT_MIN_THREAD", prop));
			UNIFIED_AGENT_MAX_THREAD = Integer.parseInt(getProperty("UNIFIED_AGENT_MAX_THREAD", prop));
			
			NET_STACK_AGENT_SERVICE_RUNTIME_INTERVAL_MS = Long.parseLong(getProperty("NET_STACK_AGENT_SERVICE_RUNTIME_INTERVAL_MS", prop));
			NET_STACK_AGENT_MIN_THREAD = Integer.parseInt(getProperty("NET_STACK_AGENT_MIN_THREAD", prop));
			NET_STACK_AGENT_MAX_THREAD = Integer.parseInt(getProperty("NET_STACK_AGENT_MAX_THREAD", prop));
			
			NET_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS = Long.parseLong(getProperty("NET_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS", prop));
			JAVA_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS = Long.parseLong(getProperty("JAVA_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS", prop));
			
			NET_STACK_DATA_SERVICE_MIN_THREAD = Integer.parseInt(getProperty("NET_STACK_DATA_SERVICE_MIN_THREAD", prop));
			NET_STACK_DATA_SERVICE_MAX_THREAD = Integer.parseInt(getProperty("NET_STACK_DATA_SERVICE_MAX_THREAD", prop));
			
			//Added below for JStack related changes.
			JAVA_STACK_DATA_SERVICE_MIN_THREAD = Integer.parseInt(getProperty("JAVA_STACK_DATA_SERVICE_MIN_THREAD", prop));
			JAVA_STACK_DATA_SERVICE_MAX_THREAD = Integer.parseInt(getProperty("JAVA_STACK_DATA_SERVICE_MAX_THREAD", prop));
			
			APP_LIST_FILE_PATH = getProperty("APP_LIST_FILE_PATH", prop);
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			prop.clear();
			
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Loads AppedoConstants, 
	 * of loads Appedo whitelabels, replacement of word `Appedo` as configured in DB
	 * 
	 * @param con
	 * @param strAppedoConfigPath - Not in use
	 * @throws Throwable
	 */
	public static void loadAppedoConstants(Connection con) throws Throwable {
		
		try {
			// 
			AppedoConstants.getAppedoConstants().loadAppedoConstants(con);
			
		} catch (Throwable th) {
			throw th;
		}
	}
	
	/**
	 * Get the property's value from appedo_config.properties, if it is DEV environment;
	 * Otherwise get the property's value from DB.
	 * 
	 * @param strPropertyName
	 * @param prop
	 * @return
	 */
	private static String getProperty(String strPropertyName, Properties prop) throws Throwable {
		if( DEV_ENVIRONMENT && prop.getProperty(strPropertyName) != null )
			return prop.getProperty(strPropertyName);
		else
			return AppedoConstants.getAppedoConfigProperty(strPropertyName);
	}
}
