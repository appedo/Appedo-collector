package com.appedo.webcollector.webserver.resource;

import net.sf.json.JSONObject;

import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;

import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.manager.CollectorManager;

public class NotificationDataCollectorResource extends Resource {

	public NotificationDataCollectorResource(Context context, Request request, Response response) {
		super(context, request, response);

		// Declare the kind of representations supported by this resource.
		getVariants().add(new Variant(MediaType.APPLICATION_JSON));

		// Allow modifications of this resource via POST requests.
		setModifiable(true);
	}
	
	public void acceptRepresentation(Representation entity) throws ResourceException {
		
		//StringBuilder sbXML = null;
		JSONObject joCounterData = null;
		CollectorManager collectorManager = null;
		boolean bQueued = false;
		
		try {
			//System.out.println("In Appedo Notification Collector : ");
			String rawData = entity.getText();
		//	System.out.println("request : "+rawData);
			if (rawData.startsWith("{") && rawData.endsWith("}")) {
				joCounterData = JSONObject.fromObject(rawData);
				if (joCounterData.containsKey("notification_message")) {
					collectorManager = CollectorManager.getCollectorManager();
					bQueued = collectorManager.collectNotificationData(joCounterData);
					
					if( bQueued ) {
						//LogManager.infoLog("Data Queued.");
					} else {
						LogManager.errorLog("Unable to queue data.");
					}
				}
			} else {
				LogManager.errorLog("Invalid JSON data : "+rawData);
			}
			
		} catch (Exception e) {
			LogManager.errorLog(e);			
			getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
			//sbXML = UtilsFactory.getJSONFailureReturn( e.getMessage() );
		}
		
		//Representation rep = new StringRepresentation(sbXML);
		//getResponse().setEntity(rep);
	}
}
