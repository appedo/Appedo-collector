package com.appedo.webcollector.webserver.resource;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;

import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.CollectorManager;
import com.appedo.webcollector.webserver.manager.ModulePerformanceCounterManager;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * Performance Counter Collector Resource
 * This service will receive all the counter data and pass it to respective Manager.
 * 
 * @author Ramkumar
 *
 */
public class PCCollectorResourceV2 extends Resource {
	
	public PCCollectorResourceV2(Context context, Request request, Response response) {
		super(context, request, response);
		//System.out.println("collectorResource: "+request.getEntity().toString());
		
		// Declare the kind of representations supported by this resource.
		getVariants().add(new Variant(MediaType.MULTIPART_FORM_DATA));

		// Allow modifications of this resource via POST requests.
		setModifiable(true);
	}
	
	@Override
	/**
	 * Handle POST requests: Receive the Counter data with agent_type
	 * 
	 * @param entity
	 *			Form entity
	 * @throws ResourceException
	 */
	public void acceptRepresentation(Representation entity) throws ResourceException {
		
		StringBuilder responseXml = null;
		
		JSONObject joCounters = null;
		try{
			joCounters = new JSONObject();
			joCounters = JSONObject.fromObject(entity.getText());
			//System.out.println(joCounters);
			
			System.out.println("-------New Reselio Agent------");
			System.out.println("command :"+joCounters.getString("command"));
		} catch(Exception e){
			LogManager.errorLog(e);
			System.out.println(e);
		}
		System.out.println("PCCollectorResourceV2");

	
		
		responseXml = collectAgentMessage(joCounters);
		
		Representation rep = new StringRepresentation(responseXml);
		getResponse().setEntity(rep);
	}
	
	
	/**
	 * Process edit profile headers
	 * 
	 * @param userid
	 * @return
	 */
	private StringBuilder collectAgentMessage(JSONObject joCounterParams) {
		StringBuilder sbXML = null;
		
		JSONObject joResponse = null;
		CollectorManager collectorManager = null;
		Connection con = null;
		ModulePerformanceCounterManager counterManager = null;
		HashMap<String, Object> hmResponse = new HashMap<String, Object>();
		boolean isGuidExists = false;
		try {
			System.err.println(System.currentTimeMillis());
			System.out.println("collectorAgentMessage function!");
			collectorManager = CollectorManager.getCollectorManager();
			counterManager = new ModulePerformanceCounterManager();
			joResponse = new JSONObject();
			
			if(!joCounterParams.containsKey("oldGuid") && joCounterParams.getString("oldGuid").isEmpty()){
				joResponse = counterManager.insertAndCreateModulesV2(joCounterParams, joCounterParams.getString("command"), joCounterParams.getString("uuid"));
			}else{
				isGuidExists = counterManager.isGuidExist(joCounterParams.getString("oldGuid"));
				if(isGuidExists){
					joResponse = counterManager.migrationCounterModulesV2(joCounterParams, joCounterParams.getString("command"), joCounterParams.getString("uuid"));
				}else{
					joResponse = counterManager.insertAndCreateModulesV2(joCounterParams, joCounterParams.getString("command"), joCounterParams.getString("uuid"));
				}
			}
			
			hmResponse.put(joCounterParams.getString("command"), joResponse.getJSONObject("joResponse"));
			sbXML = UtilsFactory.getJSONSuccessReturn(hmResponse);
			System.err.println(System.currentTimeMillis());	
		} catch (Exception e) {
			LogManager.errorLog(e);			
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		} finally {
			DataBaseManager.close(con);
			con = null;
		}
		
		System.out.println("Response :" +sbXML.toString());
		return sbXML;
	}
	
	/**
	 * main function to test this resource
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		
		//logger.debug("Entered into : getUser: getDocForRequest "+ requestUrl);
		HttpClient client = null;
		String responseJSONStream = null;
		
		try{
			client = new HttpClient();
			// URLEncoder.encode(requestUrl,"UTF-8");
			//PostMethod method = new PostMethod("http://localhost:8080/SS_Profiler/collectCounters");
			PostMethod method = new PostMethod("http://localhost:8080/Appedo-Collector/collectCountersV2");
			//method.setRequestBody("{\"command\":\"WINDOWS\",\"uuid\":\"4C4C4544-0034-4410-804E-B8C04F363332\",\"oldGuid\":\"\",\"eid\":\"3\",\"version\":\"Microsoft Windows NT 6.2.9200.0\",\"datetime\":\"2017-06-05T15:20:42.7760529+05:30\",\"counterSet\":{\"counterData\":[{\"category\":\"TCPv4\",\"counter_name\":\"Segments/sec\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"CountPerSec\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Segments/sec,\",\"counter_description\":\"Segments/sec is the rate at which TCP segments are sent or received using the TCP protocol.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Connections Established\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"number\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Connections Established,\",\"counter_description\":\"Connections Established is the number of TCP connections for which the current state is either ESTABLISHED or CLOSE-WAIT.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Connections Active\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"number\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Connections Active,\",\"counter_description\":\"Connections Active is the number of times TCP connections have made a direct transition to the SYN-SENT state from the CLOSED state. In other words, it shows a number of connections which are initiated by the local computer. The value is a cumulative total.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Connections Passive\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"number\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Connections Passive,\",\"counter_description\":\"Connections Passive is the number of times TCP connections have made a direct transition to the SYN-RCVD state from the LISTEN state. In other words, it shows a number of connections to the local computer, which are initiated by remote computers. The value is a cumulative total.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Connection Failures\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"number\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Connection Failures,\",\"counter_description\":\"Connection Failures is the number of times TCP connections have made a direct transition to the CLOSED state from the SYN-SENT state or the SYN-RCVD state, plus the number of times TCP connections have made a direct transition to the LISTEN state from the SYN-RCVD state.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Connections Reset\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"number\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Connections Reset,\",\"counter_description\":\"Connections Reset is the number of times TCP connections have made a direct transition to the CLOSED state from either the ESTABLISHED state or the CLOSE-WAIT state.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Segments Received/sec\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"CountPerSec\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Segments Received/sec,\",\"counter_description\":\"Segments Received/sec is the rate at which segments are received, including those received in error.  This count includes segments received on currently established connections.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Segments Sent/sec\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"CountPerSec\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Segments Sent/sec,\",\"counter_description\":\"Segments Sent/sec is the rate at which segments are sent, including those on current connections, but excluding those containing only retransmitted bytes.\"},{\"category\":\"TCPv4\",\"counter_name\":\"Segments Retransmitted/sec\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"CountPerSec\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,TCPv4,Segments Retransmitted/sec,\",\"counter_description\":\"Segments Retransmitted/sec is the rate at which segments are retransmitted, that is, segments transmitted containing one or more previously transmitted bytes.\"},{\"category\":\"NBT Connection\",\"counter_name\":\"Bytes Received/sec\",\"has_instance\":\"t\",\"instance_name\":\"\",\"unit\":\"CountPerSec\",\"is_selected\":\"f\",\"is_static_counter\":\"f\",\"query_string\":\"TRUE,NBT Connection,Bytes Received/sec,\",\"counter_description\":\"Bytes Received/sec is the rate at which bytes are received by the local computer over an NBT connection to some remote computer.  All the bytes received by the local computer over the particular NBT connection are counted.\"},{\"category\":\"Memory\",\"counter_name\":\"Page Faults/sec\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"CountPerSec\",\"is_selected\":\"t\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,Memory,Page Faults/sec,\",\"counter_description\":\"Page Faults/sec is the average number of pages faulted per second. It is measured in number of pages faulted per second because only one page is faulted in each fault operation, hence this is also equal to the number of page fault operations. This counter includes both hard faults (those that require disk access) and soft faults (where the faulted page is found elsewhere in physical memory.) Most processors can handle large numbers of soft faults without significant consequence. However, hard faults, which require disk access, can cause significant delays.\"},{\"category\":\"System\",\"counter_name\":\"Processor Queue Length\",\"has_instance\":\"f\",\"instance_name\":\"\",\"unit\":\"number\",\"is_selected\":\"t\",\"is_static_counter\":\"f\",\"query_string\":\"FALSE,System,Processor Queue Length,\",\"counter_description\":\"Processor Queue Length is the number of threads in the processor queue.  Unlike the disk counters, this counter counters, this counter shows ready threads only, not threads that are running.  There is a single queue for processor time even on computers with multiple processors. Therefore, if a computer has multiple processors, you need to divide this value by the number of processors servicing the workload. A sustained processor queue of less than 10 threads per processor is normally acceptable, dependent of the workload.\"}]}}");
	
			//method.setParameter("agent_type", "MYSQL");
			int statusCode = client.executeMethod(method);
			System.out.println("statusCode: "+statusCode);
			
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			try {
				responseJSONStream = method.getResponseBodyAsString();
				System.out.println("responseJSONStream: "+responseJSONStream);
			} catch (HttpException he) {
				LogManager.errorLog(he);
			}
		} catch (IOException ie) {
			LogManager.errorLog(ie);
		} catch (Exception e) {
			LogManager.errorLog(e);
		} finally {
			client = null;
			responseJSONStream = null;
		}
	}

}
