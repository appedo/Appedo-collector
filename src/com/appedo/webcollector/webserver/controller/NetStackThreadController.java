package com.appedo.webcollector.webserver.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.NetStackDataBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.util.Constants;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class NetStackThreadController extends Thread {

	
	private CollectorManager collectorManager = null;
	
	public NetStackThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	public void run() {
		
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("NetStackThreadController got stopped");
		LogManager.infoLog("NetStackThreadController got stopped");
		LogManager.errorLog( new Exception("NetStackThreadController got stopped") );
		
		super.finalize();
	}
}
