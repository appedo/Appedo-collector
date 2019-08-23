package com.appedo.webcollector.webserver.controller;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.Router;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.resource.FileComparison;
import com.appedo.webcollector.webserver.resource.JStackDataCollectorResource;
import com.appedo.webcollector.webserver.resource.LinuxUnification;
import com.appedo.webcollector.webserver.resource.NetStackDataCollectorResource;
import com.appedo.webcollector.webserver.resource.NotificationDataCollectorResource;
import com.appedo.webcollector.webserver.resource.PCCollectorResource;
import com.appedo.webcollector.webserver.resource.PCCollectorResourceV2;
import com.appedo.webcollector.webserver.resource.ProfilerCollectorResource;
import com.appedo.webcollector.webserver.resource.TransactionSetupResource;
import com.appedo.webcollector.webserver.resource.TransactionSetupResourceV2;
import com.appedo.webcollector.webserver.resource.UnifiedAgentCounterCollectorResource;

/**
 * Class which receives the HTTP request and routes the said resource class.
 * 
 * @author Ramkumar R
 *
 */
public class AppedoCollector_WebApplication extends Application {

	// set log access
	//private static final Category log = Category.getInstance(SPCG_WebApplication.class.getName());
	
	/**
	 * Creates a root Restlet that will receive all incoming calls.
	 */
	@Override
	public synchronized Restlet createRoot() {
		// Create a router Restlet that routes each call to a
		// specific resources based on a given URI pattern.
		Router router = new Router(getContext());
		
		LogManager.infoLog("Appedo Collector webservice");
		
		// Attach default route
		//router.attachDefault(HelloResource.class);
		
		
		// URI pattern: (POST) This service will receive all the counter data and pass it to respective Manager.
		// Sent by various Appedo-Profiler Services (or) Agents.
		router.attach("/init/reloadConfigProperties", TransactionSetupResource.class);
		
		router.attach("/getConfigurations", TransactionSetupResource.class);
		
		router.attach("/getConfigurationsV2", TransactionSetupResourceV2.class);
		
			//.extractQuery("guid", "guid", true)
			//.extractQuery("agent_type", "agent_type", true);
		
		
		// URI pattern: (POST) This service will receive all the counter data and pass it to respective Manager.
		// Sent by various SSProfiler agents.
		router.attach("/collectCounters", PCCollectorResource.class);

		router.attach("/collectCountersV2", PCCollectorResourceV2.class);
		
		//Java Unification for Server, DataBase, Application
		router.attach("/javaUnification", LinuxUnification.class);
		
		//Data from Unified Agent for collect counter data through filebeat & logstash
		router.attach("/UACountersCollect", UnifiedAgentCounterCollectorResource.class);

		//Data from Unified Agent for notification data through filebeat & logstash
		router.attach("/collectNotificationData", NotificationDataCollectorResource.class);
		
		//Data from Unified Agent for collect netstack through filebeat & logstash
		router.attach("/netStackCollect", NetStackDataCollectorResource.class);
		
		//Data from Unified Agent for collect JStack through filebeat & logstash
		router.attach("/jStackCollect", JStackDataCollectorResource.class);

		//Data from install file comparison agent 
		router.attach("/collectDataBytes", FileComparison.class);

		
		// URI pattern: (POST) This service will receive all the counter data and pass it to respective Manager.
		// Sent by various SSProfiler agents.
		router.attach("/collectProfilerStack", ProfilerCollectorResource.class);
			//.extractQuery("agent_type", "agent_type", true)
			//.extractQuery("counter_params_json", "counter_params_json", true);
		
		/*
		// URI pattern: (POST) This service will receive all the counter data and pass it to respective Manager.
		// Sent by various SSProfiler agents.
		router.attach("/getTransactionTree", ProfilerTransactionTreeResource.class);
			//.extractQuery("guid", "guid", true);

		
		// URI pattern: (POST) This service will receive all the counter data and pass it to respective Manager.
		// Sent by various SSProfiler agents.
		router.attach("/getTransactionStack", ProfilerTransactionStackResource.class);
			//.extractQuery("guid", "guid", true);
		*/
		
		return router;
	}
}
