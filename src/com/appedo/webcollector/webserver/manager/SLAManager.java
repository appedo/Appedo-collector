package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.SLADBI;
import com.appedo.webcollector.webserver.bean.SLABean;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * Manager class to do the operations related the SLA breach
 * 
 * @author Ramkumar R
 *
 */
public class SLAManager {
	
	private static SLAManager slaManager = null;
	
	// Database Connection object. Single connection will be maintained for entire JavaProfiler operations.
	private Connection conSLA = null;
	
	private ArrayList<SLABean> alSLAs = null;
	
	private ScriptEngineManager scriptEngineManager = null;
	private ScriptEngine scriptEngine = null;
	
	/**
	 * Avoid object creation from outside
	 */
	private SLAManager() {
		
		try{
			conSLA = DataBaseManager.giveConnection();
			
			scriptEngineManager = new ScriptEngineManager();
			scriptEngine = scriptEngineManager.getEngineByName("js");
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	/**
	 * Avoid multiple object creation for this Class, so returns the Singleton object created here.
	 * @return
	 */
	public static SLAManager getSLAManager(){
		if( slaManager == null ){
			slaManager = new SLAManager();
		}
		
		return slaManager;
	}
	
	/**
	 * Create SLA threads for each SLA entries configured.
	 */
	public void monitorSLA(){
		SLABean slaBean = null;
		SLAThread slaThread = null;
		SLADBI slaDBI = null;
		boolean bEval = false;
		
		try {
			conSLA = DataBaseManager.reEstablishConnection(conSLA);
			
			populateSLAs(conSLA);
			
			//System.out.println("looping in "+alSLAs.size()+" SLAs.");
			for(int nSLAIndex=0; nSLAIndex < alSLAs.size(); nSLAIndex++){
				conSLA = DataBaseManager.reEstablishConnection(conSLA);
				
				slaBean = alSLAs.get(nSLAIndex);
				
				bEval = validateSLA(slaBean);
				
				if( bEval ){
					// log the entries into to db
					slaBean.setBreachedOn(new Date());
					
					slaBean.setURLParameters( slaBean.getURLParameters()
							.replaceAll("#_CURRENT_VALUE_#", slaBean.getCounterValue()+"")
							.replaceAll("#_THRESOLD_VALUE_#", slaBean.getThresoldValue()+"") );
					
					slaDBI = new SLADBI();
					slaDBI.insertSLALog(conSLA, slaBean);
					
					slaThread = new SLAThread(slaBean);
					
					slaThread.start();
				}
			}
		} catch(Exception e) {
			LogManager.errorLog(e);
			
			//throw e;
		}finally {
			DataBaseManager.close(conSLA);
			conSLA = null;
			
			slaBean = null;
			slaThread = null;
			slaDBI = null;
		}
	}
	
	/**
	 * Get all the SLAs configured in database.
	 * 
	 * @param con
	 * @throws Exception
	 */
	private void populateSLAs(Connection con) throws Exception {
		SLADBI sladbi = null;
		
		try {
			sladbi = new SLADBI();
			
			UtilsFactory.clearCollectionHieracy(alSLAs);
			
			alSLAs = sladbi.getAllSLAs(con);
			
		} catch(Exception e) {
			LogManager.errorLog(e);			
			throw e;
		}
	}
	
	/**
	 * Validate the SLA with the last counter value stored in static SLA variables.
	 * 
	 * @param slaBean
	 * @return
	 */
	private boolean validateSLA(SLABean slaBean) {
		boolean bEvalResult = false;
		String strCounterCode = "";
		try {
			// validate SLA
			
			if( slaBean.getCounter().equals("FREEPHYSICALMEMORY") ) {
				strCounterCode = "9000003";
			}
			if( slaBean.getCounter().equals("SYSTEMCPULOAD") ) {
				strCounterCode = "9000002";
			}
			
			if( slaBean.getModuleType().equals("SERVER") // && slaBean.getCounter().equals("FREEPHYSICALMEMORY") 
					&& CollectorManager.getCollectorManager().isLatestCountersContains(strCounterCode+"_"+slaBean.getEncryptedUID()) ){
				
				Object[] obj = CollectorManager.getCollectorManager().getLatestCounters(strCounterCode+"_"+slaBean.getEncryptedUID());
				
				// confirm whether the entry is latest
				if( ((Date)obj[0]).getTime() + (1000 * 60) >= (new Date()).getTime() ){
					Double dCounterValue = (Double)obj[1];
					
					slaBean.setCounterValue(dCounterValue);
					bEvalResult = Boolean.parseBoolean( scriptEngine.eval( dCounterValue+" "+slaBean.getThresoldOperator()+" "+slaBean.getThresoldValue() ).toString() );
				}
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);
		} finally {
			strCounterCode = null;
		}
		
		return bEvalResult;
	}
}