package com.appedo.webcollector.webserver.bean;

import java.util.Date;

/**
 * Bean class used to hold the parameters of a single SLA configuration.
 * 
 * @author Ramkumar R
 *
 */
public class SLABean {
	
	private long sla_id, lReferenceId;
	
	private String strModuleType, strModuleName, strEncryptedUID, strLevel, strCounter, strThresold_operator;
	
	private double thresold_value, counter_value;
	
	private String strNotifyBy, strEmailIds, strMobileNumbers;
	
	private String strCapaURL, strURLMethod, strURLParameters;
	
	private Date dateBreachedOn = null;
	
	/**
	 * Returns the SLA-id, primarykey in database. 
	 * Returns Null for new object.
	 * 
	 * @return
	 */
	public long getSLAId() {
		return sla_id;
	}
	/**
	 * Sets the given long value as this objects SLA-id
	 * @param sla_id
	 */
	public void setSLAId(long sla_id) {
		this.sla_id = sla_id;
	}
	
	/**
	 * Returns the module type of this object.
	 * 
	 * @return
	 */
	public String getModuleType() {
		return strModuleType;
	}
	/**
	 * Sets the module type of this object.
	 * 
	 * @param strModuleType
	 */
	public void setModuleType(String strModuleType) {
		this.strModuleType = strModuleType;
	}
	
	/**
	 * Returns the module name of this object.
	 * @return
	 */
	public String getModuleName() {
		return strModuleName;
	}
	/**
	 * Sets the module name of this object.
	 * @param strModuleName
	 */
	public void setModuleName(String strModuleName) {
		this.strModuleName = strModuleName;
	}
	
	/**
	 * Returns the UID of this object.
	 * 
	 * @return
	 */
	public Long getReferenceId() {
		return lReferenceId;
	}
	/**
	 * Sets the UID of this object.
	 * @param lReferenceId
	 */
	public void setReferenceId(Long lReferenceId) {
		this.lReferenceId = lReferenceId;
	}
	
	/**
	 * Returns the UID in the encrypted format of this object.
	 * 
	 * @return
	 */
	public String getEncryptedUID() {
		return strEncryptedUID;
	}
	/**
	 * Sets the UID in the encrypted format of this object.
	 * @param strEncryptedUID
	 */
	public void setEncryptedUID(String strEncryptedUID) {
		this.strEncryptedUID = strEncryptedUID;
	}
	
	/**
	 * Returns the SLA level of this object.
	 * 
	 * @return
	 */
	public String getLevel() {
		return strLevel;
	}
	/**
	 * Sets the SLA level of this object.
	 * @param strLevel
	 */
	public void setLevel(String strLevel) {
		this.strLevel = strLevel;
	}
	
	/**
	 * Returns the counter used for the SLA operation.
	 * 
	 * @return
	 */
	public String getCounter() {
		return strCounter;
	}
	/**
	 * Sets the counter used for the SLA operation.
	 * @param strCounter
	 */
	public void setCounter(String strCounter) {
		this.strCounter = strCounter;
	}
	
	/**
	 * Returns the threshold operator used for the SLA operation.
	 * 
	 * @return
	 */
	public String getThresoldOperator() {
		return strThresold_operator;
	}
	/**
	 * Sets the threshold operator used for the SLA operation.
	 * @param strThresold_operator
	 */
	public void setThresoldOperator(String strThresold_operator) {
		this.strThresold_operator = strThresold_operator;
	}
	
	/**
	 * Returns the threshold value to be compared with then counter value.
	 * 
	 * @return
	 */
	public double getThresoldValue() {
		return thresold_value;
	}
	/**
	 * Sets the threshold value to be compared with then counter value.
	 * @param dThresold_value
	 */
	public void setThresoldValue(double dThresold_value) {
		this.thresold_value = dThresold_value;
	}
	
	/**
	 * Returns the then counter value
	 * 
	 * @return
	 */
	public double getCounterValue() {
		return counter_value;
	}
	/**
	 * Sets the then counter value
	 * 
	 * @param dCounter_value
	 */
	public void setCounterValue(double dCounter_value) {
		this.counter_value = dCounter_value;
	}
	
	/**
	 * Returns the notification method of this object.
	 * 
	 * @return
	 */
	public String getNotifyBy() {
		return strNotifyBy;
	}
	/**
	 * Set the notification method of this object.
	 * 
	 * @param strNotifyBy
	 */
	public void setNotifyBy(String strNotifyBy) {
		this.strNotifyBy = strNotifyBy;
	}
	
	/**
	 * Returns the emailIds to which SLA breached message should be mailed
	 * 
	 * @return
	 */
	public String getEmailIds() {
		return strEmailIds;
	}
	/**
	 * Sets the emailIds to which SLA breached message should be mailed
	 * @param strEmailIds
	 */
	public void setEmailIds(String strEmailIds) {
		this.strEmailIds = strEmailIds;
	}
	
	/**
	 * Returns the mobile numbers to which SLA breached message SMS should be sent
	 * 
	 * @return
	 */
	public String getMobileNumbers() {
		return strMobileNumbers;
	}
	/**
	 * Sets the mobile numbers to which SLA breached message SMS should be sent
	 * 
	 * @param strMobileNumbers
	 */
	public void setMobileNumbers(String strMobileNumbers) {
		this.strMobileNumbers = strMobileNumbers;
	}
	
	/**
	 * Returns the Corrective-Action/Preventive-Action URL, which has to be called once SLA is breached
	 * 
	 * @return
	 */
	public String getCapaURL() {
		return strCapaURL;
	}
	/**
	 * Sets the Corrective-Action/Preventive-Action URL, which has to be called once SLA is breached
	 * 
	 * @param strCapaURL
	 */
	public void setCapaURL(String strCapaURL) {
		this.strCapaURL = strCapaURL;
	}
	
	/**
	 * Returns the Corrective-Action/Preventive-Action URL's method POST/GET, which has to be called once SLA is breached
	 * 
	 * @return
	 */
	public String getURLMethod() {
		return strURLMethod;
	}
	/**
	 * Sets the Corrective-Action/Preventive-Action URL's method POST/GET, which has to be called once SLA is breached
	 * 
	 * @param strURLMethod
	 */
	public void setURLMethod(String strURLMethod) {
		this.strURLMethod = strURLMethod;
	}
	
	/**
	 * Returns the Corrective-Action/Preventive-Action URL's parameters, which has to be called once SLA is breached
	 * 
	 * @return
	 */
	public String getURLParameters() {
		return strURLParameters;
	}
	/**
	 * Sets the Corrective-Action/Preventive-Action URL's parameters, which has to be called once SLA is breached
	 * 
	 * @param strURLParameters
	 */
	public void setURLParameters(String strURLParameters) {
		this.strURLParameters = strURLParameters;
	}
	
	/**
	 * Returns the SLA breached date-time
	 * 
	 * @return
	 */
	public Date getBreachedOn() {
		return dateBreachedOn;
	}
	/**
	 * Sets the SLA breached date-time, mostly with then current date-time.
	 * 
	 * @param dateBreachedOn
	 */
	public void setBreachedOn(Date dateBreachedOn) {
		this.dateBreachedOn = dateBreachedOn;
	}
}
