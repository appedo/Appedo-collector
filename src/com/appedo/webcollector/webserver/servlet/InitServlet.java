package com.appedo.webcollector.webserver.servlet;

import java.io.IOException;
import java.sql.Connection;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.controller.BeatsThreadController;
import com.appedo.webcollector.webserver.controller.CollectorThreadController;
import com.appedo.webcollector.webserver.controller.JavaProfilerThreadController;
import com.appedo.webcollector.webserver.controller.MSSQLProcedureThreadController;
import com.appedo.webcollector.webserver.controller.MSSQLSlowQueryThreadController;
import com.appedo.webcollector.webserver.controller.MySQLSlowQueryThreadController;
import com.appedo.webcollector.webserver.controller.NotificationDataThreadController;
import com.appedo.webcollector.webserver.controller.OracleSlowQueryThreadController;
import com.appedo.webcollector.webserver.controller.PGSlowQueryThreadController;
import com.appedo.webcollector.webserver.controller.UnifiedAgentThreadController;
import com.appedo.webcollector.webserver.manager.AWSJavaMail;
import com.appedo.webcollector.webserver.manager.BackupTimer;
import com.appedo.webcollector.webserver.timer.JStackTimerTask;
import com.appedo.webcollector.webserver.timer.NetStackTimerTask;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.TaskExecutor;

/**
 * This class does the initialization operation for this application
 * 
 * @author Ramkumar R
 *
 */
public class InitServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	public static String version = null;
	public static String realPath = null;
	
	public static TimerTask ttBackup = null;
	public static Timer timerBackup = null;
	public static TimerTask timerUpgradeModule = null;
	public static Timer timerUpgrade = new Timer();
	
	public static TimerTask ttNetStackService = null;
	public static Timer timerNetStackService = new Timer();
	
	public static TimerTask ttJStackService = null;
	public static Timer timerJStackService = new Timer();
	
	public static TimerTask ttNetStackContinutyFillerService = null;
	public static Timer timerNetStackContinutyFillerService = new Timer();
	
	/**
	 * Do the initialization operation of this application like:
	 * - Get the configuration data from configuration files.
	 * - Schedule & Start all the counter class timers.
	 * 
	 */
	public void init() {
		//super();
		System.out.println("Initializing Appedo Collector Web App...");
		
		Connection con = null;
		
		// declare Servlet context
		ServletContext context = getServletContext();
		
		realPath = context.getRealPath("//");
		version = (String) context.getInitParameter("version");
		
		try{
			String strConstantsFilePath = context.getInitParameter("CONFIG_PROPERTIES_FILE_PATH");
			String strLog4jFilePath = context.getInitParameter("LOG4J_PROPERTIES_FILE_PATH");
			
			Constants.CONSTANTS_FILE_PATH = InitServlet.realPath+strConstantsFilePath;
			Constants.LOG4J_PROPERTIES_FILE = InitServlet.realPath+strLog4jFilePath;
			
			
			// Loads log4j configuration properties
			LogManager.initializePropertyConfigurator( Constants.LOG4J_PROPERTIES_FILE );
			
			// Loads Constant properties 
			Constants.loadConstantsProperties(Constants.CONSTANTS_FILE_PATH);
			
			// Loads db config
			DataBaseManager.doConnectionSetupIfRequired("Appedo-Collector", Constants.APPEDO_CONFIG_FILE_PATH, true);
			
			con = DataBaseManager.giveConnection();
			
			// loads Appedo constants: WhiteLabels, Config-Properties
			Constants.loadAppedoConstants(con);
			
			// Loads Appedo config properties from the system path
			Constants.loadAppedoConfigProperties(Constants.APPEDO_CONFIG_FILE_PATH);
			
			// Loads mail config
			AWSJavaMail.getManager().loadPropertyFileConstants(Constants.SMTP_MAIL_CONFIG_FILE_PATH);
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		try{
			
		//	Constants.UNIFIED_COUNTER_TIMER_CHECK = System.currentTimeMillis();
		//	Constants.NOTIFICATION_DATA_TIMER_CHECK = System.currentTimeMillis();
			
			// Create Thread pool for unified agent to manage threads
			TaskExecutor.newExecutor(Constants.UNIFIED_AGENT_THREADPOOL_NAME, Constants.UNIFIED_AGENT_MIN_THREAD, Constants.UNIFIED_AGENT_MAX_THREAD, Constants.UNIFIED_AGENT_QUEUE_SIZE);
			
			// Create Thread pool for NET-stack agent to manage threads
			//TaskExecutor.newExecutor(Constants.NET_STACK_AGENT_THREADPOOL_NAME, Constants.NET_STACK_AGENT_MIN_THREAD, Constants.NET_STACK_AGENT_MAX_THREAD, Constants.NET_STACK_AGENT_QUEUE_SIZE);
			
			// Create Thread pool for J-stack agent to manage threads
			TaskExecutor.newExecutor(Constants.JAVA_STACK_DATA_SERVICE_THREADPOOL_NAME, Constants.JAVA_STACK_DATA_SERVICE_MIN_THREAD, Constants.JAVA_STACK_DATA_SERVICE_MAX_THREAD, Constants.JAVA_STACK_DATA_SERVICE_QUEUE_SIZE);

			// Create Thread pool for NET-stack Data to manage threads
			TaskExecutor.newExecutor(Constants.NET_STACK_DATA_SERVICE_THREADPOOL_NAME, Constants.NET_STACK_DATA_SERVICE_MIN_THREAD, Constants.NET_STACK_DATA_SERVICE_MAX_THREAD, Constants.NET_STACK_DATA_SERVICE_QUEUE_SIZE);
			
			for( int i=0; i<Constants.MODULE_CONTROLLER_THREADS; i++ ) {
				(new CollectorThreadController()).start();
			}
			//To be commented after testing..	
			/*for( int i=0; i<Constants.MODULE_CONTROLLER_THREADS; i++ ) {
				(new CollectorThreadNewController()).start();
			}*/
			
			for( int i=0; i<Constants.UNIFIED_AGENT_COLLECTOR_THREADS; i++ ) {
				(new UnifiedAgentThreadController()).start();
			}
			
			for( int i=0; i<Constants.NOTIFICATION_DATA_COLLECTOR_THREADS; i++ ) {
				(new NotificationDataThreadController()).start();
			}
			
			for( int i=0; i<Constants.JAVA_PROFILER_CONTROLLER_THREADS; i++ ) {
				(new JavaProfilerThreadController()).start();
			}
			
			// DotNet Profiler: Compute the Method Stack Trace, which is already saved in dotnet_profiler_method_trace_<UID>
			/*for( int i=0; i<Constants.DOTNET_PROFILER_STACKTRACE_COMPUTE_THREADS; i++ ) {
				(new DotNetProfilerThreadController("STACKTRACE_COMPUTE")).start();
			}*/
			
			// DotNet Profiler: Insert the computed datum, which are available in the HashTable & Stack
			/*for( int i=0; i<Constants.DOTNET_PROFILER_STACKTRACE_INSERT_THREADS; i++ ) {
				(new DotNetProfilerThreadController("STACKTRACE_INSERT")).start();
			}*/
			
			for( int i=0; i<Constants.PG_PROFILER_CONTROLLER_THREADS; i++ ) {
				(new PGSlowQueryThreadController()).start();
			}
			
			for( int i=0; i<Constants.MSSQL_PROFILER_CONTROLLER_THREADS; i++ ) {
				(new MSSQLSlowQueryThreadController()).start();
			}
			
			for( int i=0; i<Constants.MSSQL_PROCEDURE_CONTROLLER_THREADS; i++ ) {
				(new MSSQLProcedureThreadController()).start();
			}
			
			for( int i=0; i<Constants.MYSQL_PROCEDURE_CONTROLLER_THREADS; i++ ) {
				(new MySQLSlowQueryThreadController()).start();
			}
			for( int i=0; i<Constants.ORACLE_PROFILER_CONTROLLER_THREADS; i++ ) {
				(new OracleSlowQueryThreadController()).start();
			}
			for( int i=0; i<Constants.BEATS_CONTROLLER_THREADS; i++ ) {
				(new BeatsThreadController()).start();
			}
			
			//TimerTask for AlertLog services, will start in 100ms with interval of X ms(configured in appedo_config properties)
			//ttUnifiedAgentService = new UnifiedAgentTimerTask();
			//timerUnifiedAgentService.schedule(ttUnifiedAgentService, 1000, Constants.UNIFIED_AGENT_SERVICE_RUNTIME_INTERVAL_MS);
			
			//TimerTask for Net Stack data services, will start in 1000ms with interval of X ms(configured in appedo_config properties)
			ttNetStackService = new NetStackTimerTask();
			timerNetStackService.schedule(ttNetStackService, 1000, Constants.NET_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS);
			
			//TimerTask for JStack data services, will start in 1000ms with interval of X ms(configured in appedo_config properties)
			ttJStackService = new JStackTimerTask();
			timerJStackService.schedule(ttJStackService, 1000, Constants.JAVA_STACK_DATA_SERVICE_RUNTIME_INTERVAL_MS);

			//Commented below Timer Task as it doesn't require as per new Net Stack aggregator flow
			//ttNetStackContinutyFillerService = new NetStackContinuityFillerTimerTask();
			//timerNetStackContinutyFillerService.schedule(ttNetStackContinutyFillerService, 1000, Constants.NET_STACK_AGENT_SERVICE_RUNTIME_INTERVAL_MS);
			
			// To Check & upgrade monitor agent
			//timerUpgradeModule = new UpgradeTimerTask();
			//timerUpgrade.schedule(timerUpgradeModule, 500, Constants.UPGRADE_CHECK_INTERVAL * 1000);
			
			// ----- BACKUP -----
			// Start perf.counter db insert after told time
			ttBackup = new BackupTimer();
			timerBackup = new Timer();
			
		} catch(Throwable e) {
			LogManager.errorLog(e);
		}
	}
	
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}
}
