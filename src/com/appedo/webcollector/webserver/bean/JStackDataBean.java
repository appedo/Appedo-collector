package com.appedo.webcollector.webserver.bean;

import net.sf.json.JSONObject;

public class JStackDataBean implements Comparable<JStackDataBean> {

	private Long dateQueuedOn = null;
	private JSONObject joJStackData = new JSONObject();
	
	public Long getDateQueuedOn() {
		return dateQueuedOn;
	}
	public void setDateQueuedOn(Long dateQueuedOn) {
		this.dateQueuedOn = dateQueuedOn;
	}
	
	public JSONObject getJStackData() {
		return joJStackData;
	}
	public void setJStackData(JSONObject joJStackData) {
		this.joJStackData = joJStackData;
	}
	
	@Override
	public int compareTo(JStackDataBean another) {
		// compareTo should return < 0 if this is supposed to be
		// less than other, > 0 if this is supposed to be greater than 
		// other and 0 if they are supposed to be equal
		
		return ((int) (getDateQueuedOn() - another.getDateQueuedOn()));
	}
}
