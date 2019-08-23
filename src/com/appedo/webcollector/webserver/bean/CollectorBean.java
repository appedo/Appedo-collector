package com.appedo.webcollector.webserver.bean;

import java.util.Date;

import net.sf.json.JSONObject;

/**
 * This Bean is to hold the counter-sets as String, along with its received date-time.
 * 
 * @author Ramkumar R
 *
 */
public class CollectorBean implements Comparable<CollectorBean> {
	private String strCounterParams = null;
	private JSONObject joContersParams = null;
	private String strModuleName = null;
	private String strUUID = null;
	private String strModuleVersion = null;
	private String strEnterpriseId = null;
	private String strOldGUID = null;

	private Date dateReceivedOn = null;
	
	/**
	 * Returns the Counter-sets which is received as string in the HTTP request.
	 * 
	 * @return
	 */
	public String getCounterParams() {
		return strCounterParams;
	}
	
	/**
	 * Sets the Counter-sets to the bean member.
	 * 
	 * @param strCounterParams
	 */
	public void setCounterParams(String strCounterParams) {
		this.strCounterParams = strCounterParams;
	}
	
	/**
	 * Returns the Counter-sets which is received as string in the HTTP request.
	 * 
	 * @return
	 */
	public JSONObject getJSONCounterParams() {
		return joContersParams;
	}

	public String getStrModuleName() {
		return strModuleName;
	}
	
	public void setStrModuleName(String strModuleName) {
		this.strModuleName = strModuleName;
	}
	
	public String getStrUUID() {
		return strUUID;
	}

	public void setStrUUID(String strUUID) {
		this.strUUID = strUUID;
	}
	
	/**
	 * Sets the Counter-sets to the bean member.
	 * 
	 * @param joCounterParams
	 */
	public void setJSONCounterParams(JSONObject joCounterParams) {
		this.joContersParams = joCounterParams;
	}
	
	public String getStrModuleVersion() {
		return strModuleVersion;
	}

	public void setStrModuleVersion(String strModuleVersion) {
		this.strModuleVersion = strModuleVersion;
	}

	public String getStrEnterpriseId() {
		return strEnterpriseId;
	}

	public void setStrEnterpriseId(String strEnterpriseId) {
		this.strEnterpriseId = strEnterpriseId;
	}
	
	public String getStrOldGUID() {
		return strOldGUID;
	}

	public void setStrOldGUID(String strOldGUID) {
		this.strOldGUID = strOldGUID;
	}
	/**
	 * Returns the date-time when this counter-set is received.
	 * 
	 * @return
	 */
	public Date getReceivedOn() {
		return dateReceivedOn;
	}
	/**
	 * Sets the given Date object to the bean member.
	 * 
	 * @param dateReceivedOn
	 */
	public void setReceivedOn(Date dateReceivedOn) {
		this.dateReceivedOn = dateReceivedOn;
	}
	
	@Override
	/**
	 * Overridden the compareTo, with the received date, to queue the bean in PriorityBlockingQueue.
	 * 
	 */
	public int compareTo(CollectorBean other){
		// compareTo should return < 0 if this is supposed to be
		// less than other, > 0 if this is supposed to be greater than 
		// other and 0 if they are supposed to be equal
		long lDiff = dateReceivedOn.getTime() - other.getReceivedOn().getTime();
		if( lDiff != 0 ){
			return 1;
		}
		
		return strCounterParams.compareTo(other.getCounterParams());
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(strCounterParams+" <> appedoReceivedOn: "+dateReceivedOn);
		
		return sb.toString();
	}
	
	/**
	 * Destroy the members o this class.
	 */
	public void destory() {
		this.strCounterParams = null;
		this.dateReceivedOn = null;
	}
	
	@Override
	protected void finalize() throws Throwable {
		this.strCounterParams = null;
		this.dateReceivedOn = null;
		
		super.finalize();
	}
}
