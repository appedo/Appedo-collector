package com.appedo.webcollector.webserver.util;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimeZone;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.bean.CollectorBean;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Class which do some utilization operation.
 * 
 * @author Ramkumar R
 *
 */
public class UtilsFactory{
	
	/**
	 * Returns the current date-time in yyyy-MM-dd HH:mm:ss format.
	 * 
	 * @return
	 */
	public static String nowFormattedDate(){
		String opDate = "";
		try{
			Calendar calNow = Calendar.getInstance();
			DateFormat opFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			opDate = opFormatter.format(calNow.getTime());
		}catch(Exception e){
			LogManager.errorLog(e);
		}
		return opDate;
	}
	
	public static String nowFormattedDate(boolean bNeedMilliSec){
		String opDate = "";
		try{
			Calendar calNow = Calendar.getInstance();
			DateFormat opFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
			opDate = opFormatter.format(calNow.getTime());
		}catch(Exception e){
			LogManager.errorLog(e);
		}
		return opDate;
	}
	
	/**
	 * Returns the current date in yyyy-MM-dd format.
	 * 
	 * @return
	 */
	public static String nowYYYYMMDD(){
		String opDate = "";
		try{
			Calendar calNow = Calendar.getInstance();
			DateFormat opFormatter = new SimpleDateFormat("yyyy-MM-dd");
			opDate = opFormatter.format(calNow.getTime());
		}catch(Exception e){
			LogManager.errorLog(e);
		}
		return opDate;
	}
	
	/**
	 * Returns the given date-time string without timezone information.
	 * 
	 * @param ipDate
	 * @return
	 */
	public static String formatDateTimeToyyyyMMddHHmmss(String ipDate){
		String opDate = "";
		try{
			DateFormat ipFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");
			DateFormat opFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
			
			if( ipDate != null && ipDate.length()>0 ) {
				// set TimeZone for the formatter, which is of the input date.
				ipFormatter.setTimeZone(TimeZone.getTimeZone( ipDate.substring( ipDate.lastIndexOf(' ')+1 ) ));
				
				// format the TimeStamp to the required format
				opDate = opFormatter.format(ipFormatter.parse(ipDate));
			}
		}catch(Exception e){
			LogManager.errorLog(e);
		}
		return opDate;
	}
	
	
	/*public static Long formatISOTimeZoneToLong(String ipDate){
		Long opEpochTime = -1L;
		String omittingMillsecValues;
		try{
			if (ipDate != null && ipDate.contains(".") && (ipDate.indexOf(".")+8 < ipDate.length())){
				omittingMillsecValues = ipDate.substring(ipDate.indexOf(".")+4, ipDate.indexOf(".")+8);
				//System.out.println("check : "+check);
				ipDate = ipDate.replace(omittingMillsecValues, "");
				DateFormat ipFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
				
				if( ipDate != null && ipDate.length()>0 ) {
					opEpochTime = ipFormatter.parse(ipDate).getTime();
					//System.out.println(ipFormatter.parse(ipDate));
				}
			}
		}catch(Exception e){
			LogManager.errorLog(e);
		//	System.out.println(e.getMessage());
		}
		return opEpochTime;
	}*/
	
	/*public static void main(String args[]) throws Exception {
		System.out.println(formatISOTimeZoneToLong("2017-06-07T09:57:53.077+05:30"));
	}*/
	
	public static Long formatISOTimeZoneToLong(String ipDate){
		Long opEpochTime = -1L;
		try{
			DateFormat ipFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
			
			if( ipDate != null && ipDate.length()>0 ) {
				opEpochTime = ipFormatter.parse(ipDate).getTime();
				//System.out.println(ipFormatter.parse(ipDate));
			}
		}catch(Exception e){
			LogManager.errorLog(e);
		//	System.out.println(e.getMessage());
		}
		return opEpochTime;
	}
	
	/**
	 * Converts the given input string of given input format to Date object.
	 * 
	 * @param stDate
	 * @param strInputFormat
	 * @return Date
	 */
	public static Date toDate(String stDate, String strInputFormat){
		Date date = null;
		DateFormat opFormatter = new SimpleDateFormat(strInputFormat);
		
		try {
			if( stDate != null ){
				date = opFormatter.parse(stDate);//opFormatter.parse(stDate);//(Date)formatter.parse(opFormatter.format(stDate));
			}
		}catch (Exception e){
			LogManager.errorLog(e);
			date = null;
		}
		return date;
	}
	
	/**
	 * Format the given Epoch-in-millisecond into "yyyy-MM-dd"
	 * 
	 * @param lTime
	 * @return
	 */
	public static String formatYYYYMMDD(long lTime){
		String opDate = "";
		try{
			Calendar calNow = Calendar.getInstance();
			calNow.setTimeInMillis( lTime );
			
			DateFormat opFormatter = new SimpleDateFormat("yyyy-MM-dd");
			opDate = opFormatter.format(calNow.getTime());
		}catch(Exception e){
			LogManager.errorLog(e);
		}
		return opDate;
	}
	
	/**
	 * This method checks whether 'val1' is null OR val1 is empty, 
	 * it will return val2,
	 * else it will return val1.
	 * 
	 * @param val1 
	 * @param val2 
	 * @return String
	 */
	public static String replaceNull(Object val1, String val2) {
		if (val1 == null || val1.toString().length() == 0)
			return val2;
		else
			return val1.toString();
	}
	
	/**
	 * This method checks whether 'val1' is null, 
	 * it will return val2,
	 * else it will return val1.
	 * 
	 * @param val1
	 * @param val2
	 * @return
	 */
	public static Long replaceNull(Long val1, Long val2) {
		if (val1 == null)
			return val2;
		else
			return val1;
	}
	
	/**
	 * Returns the input string as comfortable for SQL operations.
	 * 
	 * @param str
	 * @return
	 */
	public static String makeValidVarchar(String str) {
		if( str != null)
			return "'"+str.replaceAll("'","''").replaceAll("\\\\", "\\\\\\\\")+"'";
		else
			return null;
	}
	
	/**
	 * Closes the nested collection variable.
	 * 
	 * @param objCollection
	 */
	@SuppressWarnings("rawtypes")
	public static void clearCollectionHieracy(Object objCollection){
		try{
			if( objCollection == null ){
				
			}else if( objCollection instanceof JSONObject ) {
				JSONObject joCollection = (JSONObject)objCollection;
				Iterator it = joCollection.keySet().iterator();
				while( it.hasNext() ){
					Object str = it.next();
					clearCollectionHieracy( joCollection.get(str) );
				}
				joCollection.clear();
				joCollection = null;
			}else if( objCollection instanceof JSONArray ) {
				JSONArray jaCollection = (JSONArray)objCollection;
				for( int i=0; i < jaCollection.size(); i++ ){
					clearCollectionHieracy( jaCollection.get(i) );
				}
				jaCollection.clear();
				jaCollection = null;
			}else if( objCollection instanceof Map ) {
				Map mapCollection = (Map)objCollection;
				Iterator it = mapCollection.keySet().iterator();
				while( it.hasNext() ){
					Object str = it.next();
					clearCollectionHieracy( mapCollection.get(str) );
				}
				mapCollection.clear();
				mapCollection = null;
			}else if( objCollection instanceof List ) {
				List listCollection = (List)objCollection;
				for( int i=0; i < listCollection.size(); i++ ){
					clearCollectionHieracy( listCollection.get(i) );
				}
				listCollection.clear();
				listCollection = null;
			}else if( objCollection instanceof Queue ) {
				Queue queueCollection = (Queue)objCollection;
				for( int i=0; i < queueCollection.size(); i++ ){
					clearCollectionHieracy( queueCollection.poll() );
				}
				queueCollection.clear();
				queueCollection = null;
			}else if( objCollection instanceof StringBuilder ) {
				StringBuilder sbCollection = (StringBuilder)objCollection;
				sbCollection.setLength(0);
				sbCollection = null;
			}else if( objCollection instanceof StringBuffer ) {
				StringBuffer sbCollection = (StringBuffer)objCollection;
				sbCollection.setLength(0);
				sbCollection = null;
			}else if( objCollection instanceof CollectorBean ) {
				CollectorBean cbCollection = (CollectorBean)objCollection;
				cbCollection.destory();
				cbCollection = null;
			}
			
			objCollection = null;
		}catch(Throwable t){
			LogManager.errorLog(t);
		}
	}
	
	/**
	 * Returns a JSONObject which can be used for a request's successful response, with the given message and key-value pair appended to it.
	 * 
	 * @param strMessage
	 * @return
	 */
	public static StringBuilder getJSONSuccessReturn( String strMessage, String strkey, String strValue){
		StringBuilder sbReturn = new StringBuilder(); 
		
		sbReturn.append("{")
				.append("\"success\": true, \"failure\": false, ")
				.append("\"message\": \"").append( (strMessage+"\", ") )
				.append("\"").append(strkey).append("\": \"").append(strValue).append("\" ")
				.append("}");
		
		return sbReturn;
	}
	
	/**
	 * Returns a JSONObject which can be used for a request's successful response, with the given key-value pairs as message.
	 * 
	 * @param jo
	 * @return
	 */
	public static StringBuilder getJSONSuccessReturn( JSONObject jo ){
		
		StringBuilder sbReturn = new StringBuilder(); 
		
		sbReturn.append("{")
				.append("\"success\": true, \"failure\": false, ")
				.append("\"message\": ").append( (jo.toString()) )
				.append("}");
		
		return sbReturn;
	}
	
	/**
	 * Returns a JSONObject which can be used for a request's successful response, with the given message.
	 * 
	 * @param strMessage
	 * @return
	 */
	public static StringBuilder getJSONSuccessReturn( String strMessage ){
		StringBuilder sbReturn = new StringBuilder(); 
		
		sbReturn.append("{")
				.append("\"success\": true, \"failure\": false, ")
				.append("\"message\": \"").append( (strMessage+"\"") )
				.append("}");
		
		return sbReturn;
	}
	
	/**
	 * Returns a JSONObject which can be used for a request's successful response, with the given key-value pair appended to it.
	 * 
	 * @param strkey
	 * @param strValue
	 * @return
	 */
	public static StringBuilder getJSONSuccessReturn(String strkey, String strValue){
		StringBuilder sbReturn = new StringBuilder();
		
		sbReturn.append("{")
				.append("\"success\": true, \"failure\": false, ")
				.append("\"").append(strkey).append("\": \"").append(strValue).append("\" ")
				.append("}");
		
		return sbReturn;
	}
	
	/**
	 * Returns a JSONObject which can be used for a request's successful response, with the given key-value pairs appended to it.
	 * 
	 * @param hmKeyValues
	 * @return
	 */
	public static StringBuilder getJSONSuccessReturn(HashMap<String, Object> hmKeyValues){
		StringBuilder sbReturn = new StringBuilder();
		Iterator<String> iter = hmKeyValues.keySet().iterator();
		String strKey = null;
		Object objValue = null;
		int nIndex = 0;
		
		sbReturn.append("{")
				.append("\"success\": true, \"failure\": false, ");
		while( iter.hasNext() ){
			strKey = iter.next();
			objValue = hmKeyValues.get(strKey);
			
			if( nIndex != 0 )	sbReturn.append(", ");
			
			/*if( objValue instanceof Long || objValue instanceof Integer || objValue instanceof Float ) {
				sbReturn.append(strKey).append(": ").append(objValue);
			} else {*/
			if( objValue instanceof JSONObject || objValue instanceof JSONArray || objValue instanceof HashMap || objValue instanceof ArrayList ) {
				sbReturn.append("\"").append(strKey).append("\": ").append(objValue);
			} else {
				sbReturn.append("\"").append(strKey).append("\": \"").append(objValue).append("\"");
			}
			
			nIndex++;
		}
		
		sbReturn.append("}");
		
		return sbReturn;
	}
	
	/**
	 * Returns a JSONObject which can be used for a request's failure response with a message.
	 * 
	 * @param strMessage
	 * @return
	 */
	public static StringBuilder getJSONFailureReturn( String strMessage ){
		StringBuilder sbReturn = new StringBuilder(); 
		
		sbReturn.append("{")
				.append("\"success\": false, \"failure\": true, ")
				.append("\"errorMessage\": \"").append( (strMessage+"\"") )
				.append("}");
		
		return sbReturn;
	}
	
	/**
	 * Prints the first 10 exceptions occurred for the Batch-Execution.
	 * 
	 * @param strFunctionName
	 * @param ex
	 */
	public static void printSQLNextExceptions(String strFunctionName, Exception ex) {
		int i = 0;
		SQLException sqlExcpNext = null;
		if( ex instanceof SQLException ) {
			while( (sqlExcpNext = ((SQLException)ex).getNextException()) != null) {
				LogManager.errorLog(sqlExcpNext);
				
				// stop the loop on count of 10.
				i++;
				if( i > 10 ) {
					break;
				}
			}
		}
	}
	
	/**
	 * Prints the first 10 exceptions occurred for the Batch-Execution.
	 * 
	 * @param strFunctionName
	 * @param ex
	 */
	public static void printSQLNextExceptions(String strFunctionName, Throwable th) {
		printSQLNextExceptions(strFunctionName, (Exception)th);
	}
}
