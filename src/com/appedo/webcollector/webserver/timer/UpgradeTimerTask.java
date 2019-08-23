package com.appedo.webcollector.webserver.timer;

import java.sql.Connection;
import java.util.TimerTask;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.CollectorManager;

public class UpgradeTimerTask extends TimerTask {
	private Connection conPC = null;
	
	public UpgradeTimerTask(){		
		this.conPC = DataBaseManager.giveConnection();
	}
	
	@Override
	public void run() {
		System.out.println("UpgradeTimerTask : run()");
		try{
			// if connection is not established to db server then wait for 10 seconds
			conPC = DataBaseManager.reEstablishConnection(conPC);
			
			CollectorManager.getCollectorManager().collectUpgradeGUID(conPC);
			
			System.out.println("UpgradeTimerTask : VEEru");
		} catch(Throwable th) {
			LogManager.errorLog(th);
		} finally {
			System.out.println("UpgradeTimerTask : CLOSE");
			DataBaseManager.close(conPC);
			conPC = null;
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("UpgradeTimerTask : finalize");
		//System.out.println("Node inactiavting Thread is stopping");
		DataBaseManager.close(conPC);
		conPC = null;
		
		super.finalize();
	}

}