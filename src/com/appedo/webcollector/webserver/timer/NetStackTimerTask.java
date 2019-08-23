package com.appedo.webcollector.webserver.timer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.NetStackDataBean;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.NetStackDataTimerWriter;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.TaskExecutor;
import com.appedo.webcollector.webserver.util.UtilsFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class NetStackTimerTask extends TimerTask {

	private CollectorManager collectorManager = null;
	
	public NetStackTimerTask() {
		collectorManager = CollectorManager.getCollectorManager();
	}
	
	@Override
	public void run() {
		
		//System.out.println("NetStackTimerTask : run()");
		
		JSONArray jaNetStackData = null, jaNonNetStackData = null, jaNxtDayNetStackData = null;
		JSONObject joNetStackData = null;
		ArrayList<NetStackDataBean> alNetStackBean = null;
		TaskExecutor executor = null;  
		try {
			
		//	Set<String> hsGuid = new HashSet<String>();
			//Map<String, JSONObject> hmGuidMappedData = new HashMap<String, JSONObject>();
			
			int nQueueSize = collectorManager.getNetStackDataSize();
			
			if (nQueueSize > 0) {
				//timer start, queue count
				long dataProcStartTime = System.currentTimeMillis();
				
				jaNonNetStackData = new JSONArray();
				alNetStackBean = collectorManager.pollNetStackData();
				
				jaNetStackData = JSONArray.fromObject(alNetStackBean);
				
				do {
					jaNxtDayNetStackData = new JSONArray();
					Map<String, JSONObject> hmGuidMappedData = new HashMap<String, JSONObject>();
					
					for (int count=0; count < jaNetStackData.size(); count++) {
						 joNetStackData = jaNetStackData.getJSONObject(count).getJSONObject("netStackData");
						 
						 String strStackType = joNetStackData.getString("type");
						 //Splitting NETSTACK data from queue to process data by each date & guid.
						 //NETTRACE & NETTRACEDET will process after processing NETSTACK in single go
						 if (strStackType.equals(Constants.NET_STACK_MODULE)) {
							 
							 String strGUID = joNetStackData.getString("guid");
							 // If hashmap already has guid, then it will net stack data and 
							 //add together with current netstack data and continue the process
							 if(hmGuidMappedData.containsKey(strGUID)) {
	
								 JSONObject joGUIDStackData = hmGuidMappedData.get(strGUID);
								 
								 // Splitting next day data of the GUID to process separately
								 if (!(joGUIDStackData.getString("datetime").substring(0, 10).equals(joNetStackData.getString("datetime").substring(0, 10)))) {
									 jaNxtDayNetStackData.add(jaNetStackData.get(count));
									 continue;
								 }
								 						 
								 JSONArray jaData = joGUIDStackData.getJSONArray(strStackType);
//								 
								 jaData.addAll(joNetStackData.getJSONArray(strStackType));
//								 
								 joGUIDStackData.put(strStackType, jaData);
								 
								 //Put together all NETSTACK data in single data of same guid
//								 joGUIDStackData.getJSONArray(strStackType).addAll( joNetStackData.getJSONArray(strStackType) );
								 
								 hmGuidMappedData.put(strGUID, joGUIDStackData);
								 
							 }
							 // If the hashmap doesn't has guid, then the current data will be added without process as it doesn't require.
							 else {
								 hmGuidMappedData.put(strGUID, joNetStackData);
							 }
							 
						 } else {
							 jaNonNetStackData.add(joNetStackData);
						 }
						 
					}	
					
					//UtilsFactory.clearCollectionHieracy(jaNetStackData);
					
					jaNetStackData = jaNxtDayNetStackData;
					//jaNxtDayNetStackData.clear();
					
					//Processing netstack data for each guid by iterating map
					if(hmGuidMappedData.size() > 0) {
						for (Map.Entry<String, JSONObject> guidObject : hmGuidMappedData.entrySet()) {
							// call thread with object to insert into db
							//guidObject.getValue();
							//guidObject.getKey();
							JSONArray jaNetStack = new JSONArray();
							jaNetStack.add(guidObject.getValue());
							
							executor = TaskExecutor.getExecutor(Constants.NET_STACK_DATA_SERVICE_THREADPOOL_NAME);
							executor.submit(new NetStackDataTimerWriter(jaNetStack));
						}
					}
					//to clear hashmap
					//UtilsFactory.clearCollectionHieracy(hmGuidMappedData);
				} while(jaNetStackData.size() > 0);
				
				//Processing NETTRACE & NETTRACEDET in single go
				if (jaNonNetStackData.size() > 0) {
					//call thread with object to insert into db
					executor = TaskExecutor.getExecutor(Constants.NET_STACK_DATA_SERVICE_THREADPOOL_NAME);
					executor.submit(new NetStackDataTimerWriter(jaNonNetStackData));
				}
			//timer ends	
			long dataProcTotalTime = System.currentTimeMillis() - dataProcStartTime;
			LogManager.infoLog("NetStackDataProcFor:"+nQueueSize+"Statement:"+dataProcTotalTime);
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
		System.out.println("NetStackTimerTask : finalize");
		super.finalize();
	}
	
}
