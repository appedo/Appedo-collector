package com.appedo.webcollector.webserver.timer;

import java.sql.Connection;
import java.util.TimerTask;

import net.sf.json.JSONArray;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.controller.NetStackContinuityFillerThreadController;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.TaskExecutor;

public class NetStackContinuityFillerTimerTask extends TimerTask {
	
	@Override
	public void run() {
		JSONArray jaUserIds = null;
		CollectorDBI collectorDBI = null;
		Connection conPC = null;
		TaskExecutor executor = null;
		
		try {
			collectorDBI = new CollectorDBI();
			
			conPC = DataBaseManager.giveConnection();
			
			jaUserIds = collectorDBI.getNetStackAddedUserIds(conPC);
			
			// for each UserId, create a thread to manipulate the Continuity-Id.
			for (int count=0; count < jaUserIds.size(); count++) {
				executor = TaskExecutor.getExecutor(Constants.NET_STACK_AGENT_THREADPOOL_NAME);
				executor.submit( new NetStackContinuityFillerThreadController( jaUserIds.getJSONObject(count) ) );
			}
		} catch (Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(conPC);
			conPC = null;
			
			collectorDBI = null;
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("NetStackContinuityFillerTimerTask : finalize");
		
		super.finalize();
	}
}
