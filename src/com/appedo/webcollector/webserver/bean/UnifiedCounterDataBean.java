package com.appedo.webcollector.webserver.bean;

import net.sf.json.JSONObject;

public class UnifiedCounterDataBean implements Comparable<UnifiedCounterDataBean> {

	private Long dateQueuedOn = null;
	private JSONObject joCounterData = new JSONObject();
	
	public Long getDateQueuedOn() {
		return dateQueuedOn;
	}
	public void setDateQueuedOn(Long dateQueuedOn) {
		this.dateQueuedOn = dateQueuedOn;
	}
	
	public JSONObject getCounterData() {
		return joCounterData;
	}
	public void setCounterData(JSONObject joCounterData) {
		this.joCounterData = joCounterData;
	}
	
	@Override
	public int compareTo(UnifiedCounterDataBean another) {
		// compareTo should return < 0 if this is supposed to be
		// less than other, > 0 if this is supposed to be greater than 
		// other and 0 if they are supposed to be equal
		
		return ((int) (getDateQueuedOn() - another.getDateQueuedOn()));
	}
	
}
