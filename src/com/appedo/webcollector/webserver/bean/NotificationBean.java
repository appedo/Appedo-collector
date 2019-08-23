package com.appedo.webcollector.webserver.bean;

import net.sf.json.JSONObject;

public class NotificationBean implements Comparable<NotificationBean> {

	private Long dateQueuedOn = null;
	private JSONObject joNotificationData = new JSONObject();
	
	public Long getDateQueuedOn() {
		return dateQueuedOn;
	}
	public void setDateQueuedOn(Long dateQueuedOn) {
		this.dateQueuedOn = dateQueuedOn;
	}
	
	public JSONObject getNotificationData() {
		return joNotificationData;
	}
	public void setNotificationData(JSONObject joNotificationData) {
		this.joNotificationData = joNotificationData;
	}
	
	@Override
	public int compareTo(NotificationBean another) {
		// compareTo should return < 0 if this is supposed to be
		// less than other, > 0 if this is supposed to be greater than 
		// other and 0 if they are supposed to be equal
		
		return ((int) (getDateQueuedOn() - another.getDateQueuedOn()));
	}
	
}
