package com.appedo.webcollector.webserver.timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.JStackDataBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.JStackDataTimerWriter;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.TaskExecutor;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JStackTimerTask extends TimerTask  {

private CollectorManager collectorManager = null;
	
	public JStackTimerTask() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	public void run() {
		
		//System.out.println("JStackTimerTask : run()");
		
		JSONArray jaJStackData = null, jaNonJStackData = null, jaNxtDayJStackData = null;
		JSONObject joJStackData = null;
		ArrayList<JStackDataBean> alJStackBean = null;
		TaskExecutor executor = null;  
		try {
			
		//	Set<String> hsGuid = new HashSet<String>();
			//Map<String, JSONObject> hmGuidMappedData = new HashMap<String, JSONObject>();
			
			int nQueueSize = collectorManager.getJStackDataSize();
			//System.out.println("Queue Size :"+nQueueSize);
			if (nQueueSize > 0) {
				System.out.println("Queue Size :"+nQueueSize);
				//timer start, queue count
				long dataProcStartTime = System.currentTimeMillis();
				
				jaNonJStackData = new JSONArray();
				alJStackBean = collectorManager.pollJStackData();
				
				jaJStackData = JSONArray.fromObject(alJStackBean);
				
				do {
					jaNxtDayJStackData = new JSONArray();
					Map<String, JSONObject> hmGuidMappedData = new HashMap<String, JSONObject>();
					
					for (int count=0; count < jaJStackData.size(); count++) {
						 joJStackData = jaJStackData.getJSONObject(count).getJSONObject("JStackData");
						 
						 String strStackType = joJStackData.getString("type");
						 //Splitting JSTACK data from queue to process data by each date & guid.
						 //JTRACE & JTRACEDET will process after processing JSTACK in single go
						 if (strStackType.equals(Constants.JAVA_STACK_MODULE)) {
							 
							 String strGUID = joJStackData.getString("guid");
							 // If hashmap already has guid, then it will J stack data and 
							 //add together with current Jstack data and continue the process
							 if(hmGuidMappedData.containsKey(strGUID)) {
	
								 JSONObject joGUIDStackData = hmGuidMappedData.get(strGUID);
								 
								 // Splitting next day data of the GUID to process separately
								 if (!(joGUIDStackData.getString("datetime").substring(0, 10).equals(joJStackData.getString("datetime").substring(0, 10)))) {
									 jaNxtDayJStackData.add(jaJStackData.get(count));
									 continue;
								 }
								 						 
								 JSONArray jaData = joGUIDStackData.getJSONArray(strStackType);
//								 
								 jaData.addAll(joJStackData.getJSONArray(strStackType));
//								 
								 joGUIDStackData.put(strStackType, jaData);
								 
								 //Put together all JSTACK data in single data of same guid
//								 joGUIDStackData.getJSONArray(strStackType).addAll( joJStackData.getJSONArray(strStackType) );
								 
								 hmGuidMappedData.put(strGUID, joGUIDStackData);
								 
							 }
							 // If the hashmap doesn't has guid, then the current data will be added without process as it doesn't require.
							 else {
								 hmGuidMappedData.put(strGUID, joJStackData);
							 }
							 
						 } else {
							 jaNonJStackData.add(joJStackData);
						 }
						 
					}	
					
					//UtilsFactory.clearCollectionHieracy(jaJStackData);
					
					jaJStackData = jaNxtDayJStackData;
					//jaNxtDayJStackData.clear();
					
					//Processing Jstack data for each guid by iterating map
					if(hmGuidMappedData.size() > 0) {
						for (Map.Entry<String, JSONObject> guidObject : hmGuidMappedData.entrySet()) {
							// call thread with object to insert into db
							//guidObject.getValue();
							//guidObject.getKey();
							JSONArray jaJStack = new JSONArray();
							jaJStack.add(guidObject.getValue());
							
							executor = TaskExecutor.getExecutor(Constants.JAVA_STACK_DATA_SERVICE_THREADPOOL_NAME);
							executor.submit(new JStackDataTimerWriter(jaJStack));
						}
					}
					//to clear hashmap
					//UtilsFactory.clearCollectionHieracy(hmGuidMappedData);
				} while(jaJStackData.size() > 0);
				
				//Processing JTRACE & JTRACEDET in single go
				if (jaNonJStackData.size() > 0) {
					//call thread with object to insert into db
					executor = TaskExecutor.getExecutor(Constants.JAVA_STACK_DATA_SERVICE_THREADPOOL_NAME);
					executor.submit(new JStackDataTimerWriter(jaNonJStackData));
				}
			//timer ends	
			long dataProcTotalTime = System.currentTimeMillis() - dataProcStartTime;
			LogManager.infoLog("JStackDataProcFor:"+nQueueSize+"Statement:"+dataProcTotalTime);
			}
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
		//	uaCounterTimerWriter = null;
		//	alCounterDataBean = null;
		//	jaCounterDatas = null;
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("JStackTimerTask : finalize");
		super.finalize();
	}
}
