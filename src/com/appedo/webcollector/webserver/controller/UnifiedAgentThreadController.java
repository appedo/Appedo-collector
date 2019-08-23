package com.appedo.webcollector.webserver.controller;

import java.util.ArrayList;

import net.sf.json.JSONArray;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.UnifiedCounterDataBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.UnifiedAgentCounterTimerWriter;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.TaskExecutor;

public class UnifiedAgentThreadController extends Thread {

	long lThisThreadId = 0l;
	
	private CollectorManager collectorManager = null;
	
	//= System.currentTimeMillis();
	public UnifiedAgentThreadController() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	public void run() {
		UnifiedAgentCounterTimerWriter uaCounterTimerWriter = null;
		JSONArray jaCounterDatas = null;
		ArrayList<UnifiedCounterDataBean> alCounterDataBean = null;
		
		lThisThreadId = Thread.currentThread().getId();
		TaskExecutor executor = null;  
		
		while(true) {
			try{
				//Constants.UNIFIED_COUNTER_RUNNING_TIME = System.currentTimeMillis();
				int nQueueSize = collectorManager.getUnifiedAgentCounterSize();
				//System.out.println("current Time : "+ currentTime+", counter timer :"+Constants.UNIFIED_COUNTER_TIMER_CHECK);
				/*if (Constants.UNIFIED_COUNTER_RUNNING_TIME - Constants.UNIFIED_COUNTER_TIMER_CHECK >=10000) {
				//	System.out.println("counter time cleared");
					LogManager.infoLog("counterQueueSize:"+nQueueSize); 
					Constants.UNIFIED_COUNTER_TIMER_CHECK = System.currentTimeMillis();
				}*/
				
				if( nQueueSize == 0 ) {
					//LogManager.logControllerSleep("UnifiedAgentThreadController is sleeping...");
					Thread.sleep( Constants.COUNTER_CONTROLLER_REST_MILLESECONDS );
					//LogManager.logControllerSleep("UnifiedAgentThreadController is wokeup...");
					continue;
				}
				
				alCounterDataBean = collectorManager.pollUACounter();
				
				if(alCounterDataBean != null && alCounterDataBean.size() > 0) {
					jaCounterDatas = JSONArray.fromObject(alCounterDataBean);
					
					//Thread pool concept implemented to manage threads
					//uaCounterTimerWriter = new UnifiedAgentCounterTimerWriter(jaCounterDatas);
					//uaCounterTimerWriter.start();
					
					executor = TaskExecutor.getExecutor(Constants.UNIFIED_AGENT_THREADPOOL_NAME);
					executor.submit(new UnifiedAgentCounterTimerWriter(jaCounterDatas));
					
				}
				//To avoid CPU bursting, Delaying process for 2 millisec
				Thread.sleep(2);
			} catch(Throwable th) {
				LogManager.errorLog(th);
			} finally {
				uaCounterTimerWriter = null;
				alCounterDataBean = null;
				jaCounterDatas = null;
			}
			
		}
		
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("UnifiedAgentThreadController got stopped");
		LogManager.infoLog("UnifiedAgentThreadController got stopped");
		LogManager.errorLog( new Exception("UnifiedAgentThreadController got stopped") );
		
		super.finalize();
	}
	
	/*public static void main(String args[]) throws Exception{
		
		TaskExecutor exe = null;
		TaskExecutor.newExecutor("test", 1, 1, -1 );
		JSONArray jaCounterDatas = new JSONArray();
		int i=0;
		while (true) {
			i++;
			jaCounterDatas.add("");
			
			exe = TaskExecutor.getExecutor("test");
			
			(new UnifiedAgentThreadController()).start();
			System.out.println("before submit "+i);
			exe.submit(new UnifiedAgentCounterTimerWriter(JSONArray.fromObject(jaCounterDatas)));
			System.out.println("after submit "+i);
			Thread.sleep(500);
			if (i==10) {
				break;
			}
		}
	}*/
}
