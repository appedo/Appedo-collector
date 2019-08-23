package com.appedo.webcollector.webserver.controller;

import java.sql.Connection;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;

import net.sf.json.JSONObject;

public class NetStackContinuityFillerThreadController extends Thread {
	JSONObject joUserBreachDet = null;
	CollectorDBI collectorDBI = null;
	Connection con = null;
	
	public NetStackContinuityFillerThreadController(JSONObject joUserBreachDet) {
		this.joUserBreachDet = joUserBreachDet;
		collectorDBI = new CollectorDBI();
	}
	
	public void run() {
		try {
			con = DataBaseManager.giveConnection();
			collectorDBI.fillNetStackContinuityId(con, joUserBreachDet.getLong("uid"));
			
		} catch (Throwable th) {
			LogManager.errorLog(th);
		} finally {
			DataBaseManager.close(con);
			con = null;
			
			collectorDBI = null;
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		System.out.println("NetStackContinuityFillerThreadController : finalize");
		
		super.finalize();
	}
}
