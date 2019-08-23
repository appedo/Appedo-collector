package com.appedo.webcollector.webserver.manager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Stack;
import java.util.concurrent.LinkedBlockingQueue;

import com.appedo.commons.connect.DataBaseManager;
import com.appedo.manager.LogManager;
import com.appedo.webcollector.webserver.DBI.CollectorDBI;
import com.appedo.webcollector.webserver.DBI.DotNetProfilerDBI;
import com.appedo.webcollector.webserver.util.Constants;
import com.appedo.webcollector.webserver.util.Constants.PROFILER_KEY;
import com.appedo.webcollector.webserver.util.UtilsFactory;

/**
 * This is .Net Profiler related manager class.
 * This will queue the profile-dataset into batch and insert them into database.
 * 
 * @author Ramkumar R
 *
 */
public class DotNetProfilerManager {
	
	// Database Connection object. Single connection will be maintained for entire .Net Profiler operations.
	private Connection conDotNetProfiler = null;
	
	// DataBase Interface layer
	private DotNetProfilerDBI dotNetProfilerDBI = null;
	
	// UID & Method Traces which are under process in this object.
	private String strUID = null;
	public String getUID() {
		return strUID;
	}
	private Long lComputeSerialId = null;
	
	private ArrayList<String> alMethodTraces = null;
	
	private ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > alComputedMethodTraces = null;
	public ArrayList<Hashtable<Constants.PROFILER_KEY, Object>> getAlComputedMethodTraces() {
		return alComputedMethodTraces;
	}
	
	/**
	 * Global Stack to keep all the Method Traces, of all the GUIDs, to form back the function calls in StackList format.
	 * 			Hashtable<UID, Hashtable<ThreadId, Stack< MethodDetails > > > >
	 */
	public static Hashtable<String, Hashtable<String, Stack< Hashtable<Constants.PROFILER_KEY, Object> > > > DOTNET_PROFILER_ALL_STACKTRACES = new Hashtable<String, Hashtable<String, Stack< Hashtable<Constants.PROFILER_KEY, Object> > > >();
	
	/**
	 * Global Map to keep all the Thread's Unique-Id, of all the GUIDs, to form back the Method StackTrace.
	 * 			Hashtable<UID, Last-Unique-Thread-Id-Assigned>
	 */
	public static Hashtable<String, Long> DOTNET_PROFILER_THREAD_LAST_SERIAL = new Hashtable<String, Long>();
	/**
	 * Global Map to keep all the Thread's Unique-Id, of all the GUIDs, to form back the Method StackTrace.
	 *			Hashtable[UID, Hashtable[Thread-Pool-ThreadId, Appedo-Unique-Thread-Id (i.e. Serial no) ] ]
	 */
	public static Hashtable<String, Hashtable<String, Long> > DOTNET_PROFILER_THREAD_SERIALS = new Hashtable<String, Hashtable<String, Long> >();
	
	/**
	 * Global Map to keep all the Method Traces, of all the GUIDs, to form back the function calls in StackList format.
	 * 			Hashtable<UID, Hashtable<ThreadId, No_of_Functions_included (i.e. Serial no) > >
	 */
	public static Hashtable<String, Hashtable<String, Integer> > DOTNET_PROFILER_STACK_METHOD_SERIALS = new Hashtable<String, Hashtable<String, Integer> >();
	
	/** Global Map to keep all the Method name-id pair.
	 * 			Hashtable<UID, Hashtable<MethodId, MethodName> >
	 * 
	 */
	public static Hashtable<String, Hashtable<String, String> > DOTNET_PROFILER_METHOD_REFERENCE = new Hashtable<String, Hashtable<String, String> >();
	
	/**
	 * Global Queue to keep all the completed Method details.
	 * 			Hashtable<UID, Hashtable<MethodId, MethodName> >
	 */
	public static Hashtable<String, LinkedBlockingQueue< Hashtable<Constants.PROFILER_KEY, Object> > > DOTNET_PROFILER_COMPUTED_METHODS = new Hashtable<String, LinkedBlockingQueue< Hashtable<Constants.PROFILER_KEY, Object> > >();
	public static LinkedBlockingQueue<String> DOTNET_PROFILER_COMPUTED_METHODS_UID = new LinkedBlockingQueue<String>();
	
	/** Lock variable for the function getNextComputeTraceDetails */
	public static final String LOCK_getNextComputeTraceDetails = "LOCK";
	
	/**
	 * Avoid multiple object creation, by Singleton
	 */
	public DotNetProfilerManager() {
		
		dotNetProfilerDBI = new DotNetProfilerDBI();
	}

	/**
	 * Avoid multiple object creation, by Singleton
	 * This is used in STACKTRACE_INSERT Threads.
	 * Assigned MethodTraces will be inserted into given UID partition Tables.
	 * 
	 * @param strUID
	 * @param alComputedMethodTraces
	 */
	public DotNetProfilerManager(String strUID, ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > alComputedMethodTraces) {
		dotNetProfilerDBI = new DotNetProfilerDBI();
		
		this.strUID = strUID;
		this.alComputedMethodTraces = alComputedMethodTraces;
	}
	
	/**
	 * Create a db connection object for all the operations related to this Class.
	 */
	public void establishDBConnection() {
		try{
			DataBaseManager.close(conDotNetProfiler);
			
			conDotNetProfiler = DataBaseManager.giveConnection();
			
			// Auto-Commit must be false; as multiple inserts are done for MSIIS Profiler.
			conDotNetProfiler.setAutoCommit( false );
			
		} catch(Exception ex) {
			LogManager.errorLog(ex);
		}
	}
	
	/**
	 * Insert the CSV into DB.
	 * Inserts as bulk in Stream format.
	 * 
	 * @throws Exception
	 */
	public long insertMethodDetails(String strGUID, String strMethodsCSV) throws Exception {
		CollectorDBI collectorMan = null;
		long lUID = 0;
		long lInserted = 0;
		
		Date dateLog = LogManager.logMethodStart();
		
		try{
//			// if connection is not established to db server then wait for 10 seconds
//			if( conDotNetProfiler == null || ! DataBaseManager.isConnectionExists(conDotNetProfiler) ){
//				//System.out.println(".Net Profiler: Connection not established.");
//				Thread.sleep(10000);
//				
//				// TODO RAM; remove the below function, as it has while(true) ;
//				// implement the serial no. concept for the inputs
//				conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
//			}
//			
			// if connection is not established to db server then wait for 10 seconds
			conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
			
			collectorMan = new CollectorDBI();
			lUID = collectorMan.getApplicationUID(conDotNetProfiler, strGUID);
			
			if( lUID != -1 ) {
				// Once the DB connection is established then, do the process.
				lInserted = dotNetProfilerDBI.insertMethodCalls(conDotNetProfiler, strGUID, lUID, strMethodsCSV);
			} else {
				throw new Exception("UID not found.");
			}
			
			conDotNetProfiler.commit();
		} catch(Throwable th) {
			DataBaseManager.rollback(conDotNetProfiler);
			
			LogManager.errorLog(th);
		}
		
		LogManager.logMethodEnd(dateLog);
		
		return lInserted;
	}
	
	/**
	 * Get the next set of the Method Traces.
	 * First it will get the UID to be start computing. And then get limited MethodTrace from its Temp table.
	 * 
	 */
	public void getNextComputeTraceDetails() {
		Date dateLog = LogManager.logMethodStart();
		Object[] ob = null;
		
		try{
			synchronized ( LOCK_getNextComputeTraceDetails ) {
				conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
				
				ob = dotNetProfilerDBI.getNextComputeTraceDetails(conDotNetProfiler);
				this.strUID = (String)ob[0];
				this.lComputeSerialId = (Long)ob[1];
				
				// if a UID is given then, get the top 200 Method Traces.
				// 200 can be changed with Constants.MAX_PROFILER_METHOD_TRACE_TO_COMPUTE
				if( this.strUID != null ) {
					//System.out.println("Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+this.strUID+" <> Compute Process started.");
					LogManager.infoLog("DNP <> Thread-Id: "+Thread.currentThread().getId()+" <> UID: "+this.strUID+" <> Compute Process started.");
					
					alMethodTraces = dotNetProfilerDBI.getMethodTraceDetails(conDotNetProfiler, this.strUID);
				}
				
				// As other Threads has to keep wait, the DB changes done has to be committed first.
				commitConnection();
			}
			
			LogManager.logMethodEnd(dateLog);
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
	}
	
	/**
	 * Move the computed GUID summary entry into history table.
	 */
	public void removeSummaryEntry() throws Throwable {
		conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
		
		dotNetProfilerDBI.removeSummaryEntry(conDotNetProfiler, this.lComputeSerialId);
	}
	
	/**
	 * Do the StackTrace with the MethodTrace details available in this object.
	 */
	public void buildStackTrace() throws Throwable {
		Date dateLog = LogManager.logMethodStart();
		
		String[] saMethodDetails = null, saFunctionName = null;
		String strFunctionId = null, strFunctionName = null, strClassName = null, strMethodName = null;
		String strThreadId = null, strEphochTime = null;
		long lUniqueThreadId = 0l, lMethodSerialNo = 0, lCallerMethodId = -1;
		long lStartTime = 0, lTickValue = 0, lDuration = 0;
		
		Hashtable<String, Stack< Hashtable<Constants.PROFILER_KEY, Object> > > htUIDThreads = null;
		Stack< Hashtable<Constants.PROFILER_KEY, Object> > stackUIDThreadStack = null;
		Hashtable<Constants.PROFILER_KEY, Object> htMethodDetails = null;
		
		// Get/Load the Function Name-Id pair
		if( ! DOTNET_PROFILER_METHOD_REFERENCE.containsKey(this.strUID) ) {
			loadMethodReferences();
		}
		
		for(int index = 0; index < alMethodTraces.size(); index ++) {
			conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
			
			String strMethodDetails = alMethodTraces.get(index);
			
			saMethodDetails = strMethodDetails.split(",");
			
			/**
			 * S
			 * FE,functionid,functionName
			 * F,threadID,tickValue,functionID,ephochTime
			 * L,threadID,tickValue,functionID,ephochTime
			 * FL,functionid
			 */
			
			switch( saMethodDetails[0] ){
				case "S":	// Started/Restarted the ISS. So need to clear the variables.
					DOTNET_PROFILER_ALL_STACKTRACES.remove(this.strUID);
					
					// Clear the Function's Name-Id pairs from DB.
					try{
						
						synchronized ( DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID) ) {
							UtilsFactory.clearCollectionHieracy( DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID) );
							
							DOTNET_PROFILER_METHOD_REFERENCE.put(this.strUID, new Hashtable<String, String>());
							
							dotNetProfilerDBI.clearMethodReferences(conDotNetProfiler, this.strUID);
						}
					} catch(Throwable th) {
						LogManager.errorLog(th);
					}
					
					break;
				case "FE":	// Function was loaded into Memory. So need to keep this for the future reference.
					strFunctionId = saMethodDetails[1];
					strFunctionName = saMethodDetails[2];
					
					addMethodReference(strFunctionId, strFunctionName);
					
					break;
				case "FL":	// Function was unloaded from Memory. So need to remove this from other reference.
					strFunctionId = saMethodDetails[1];
					removeMethodReference(strFunctionId);
					
					break;
				case "E":	// Add the function entry into Virtual Stack.					
					strThreadId = saMethodDetails[1];
					lTickValue = Long.parseLong(saMethodDetails[2]);
					strFunctionId = saMethodDetails[3];
					strEphochTime = saMethodDetails[4];
					lStartTime = Long.parseLong(strEphochTime)*1000;
					
					synchronized ( DOTNET_PROFILER_ALL_STACKTRACES ) {
						// Get UID's all Thread Stacks 
						if( DOTNET_PROFILER_ALL_STACKTRACES.containsKey(this.strUID) ) {
							htUIDThreads = DOTNET_PROFILER_ALL_STACKTRACES.get(this.strUID);
						} else {
							htUIDThreads = new Hashtable<String, Stack< Hashtable<Constants.PROFILER_KEY, Object> > >();
							DOTNET_PROFILER_ALL_STACKTRACES.put(this.strUID, htUIDThreads);		// Add to the DataStructure to close synchronized block
						}
						
						// Get Thread's Stack
						if( htUIDThreads.containsKey(strThreadId) ) {
							stackUIDThreadStack = htUIDThreads.get(strThreadId);
							lCallerMethodId = (Long) stackUIDThreadStack.peek().get(PROFILER_KEY.CALLEE_METHOD_ID);
						} else {
							stackUIDThreadStack = new Stack< Hashtable<Constants.PROFILER_KEY, Object> >();
							lCallerMethodId = -1;
						}
					}
					
					lUniqueThreadId = getUniqueThreadId(conDotNetProfiler, this.strUID, strThreadId);
					
					// Get the MethodName from the Hash reference, with method-id as the input
					strFunctionName = getMethodReference(strFunctionId);
					if( strFunctionName == null ) {		// If the FunctionId is not found in the HashMap then assign "<Unable to Profile>" 
						LogManager.errorLog("Unable to find the requested FunctionName. UID: "+this.strUID+" <> Thread: "+strThreadId+" <> index: "+index+" <> Txt: "+strMethodDetails);
						strFunctionName = "<Unknown Method Name>::.";
					}
					
					saFunctionName = strFunctionName.split("::");
					strClassName = saFunctionName[0];
					strMethodName = saFunctionName[1];
					
					// Get the serial number of the Method for the UID-ThreadId pair.
					lMethodSerialNo = getNextMethodSerialNo(strThreadId);
					
					// Add the Method Details into the above obtained Stack.
//					hmProfileCounter.put(Constants.PROFILER_KEY.THREAD_ID, strThreadSerialId);
//						hmProfileCounter.put(Constants.PROFILER_KEY.TYPE, UtilsFactory.coverQuotes("SQL_METHOD"));
//					hmProfileCounter.put(Constants.PROFILER_KEY.START_TIME, new Date());
//					hmProfileCounter.put(Constants.PROFILER_KEY.DURATION_MS, 0l);
//					hmProfileCounter.put(Constants.PROFILER_KEY.CLASS_NAME, UtilsFactory.coverQuotes(strClassName.replaceAll("/", ".")));
//					hmProfileCounter.put(Constants.PROFILER_KEY.METHOD_NAME, UtilsFactory.coverQuotes(strMethodName));
//					hmProfileCounter.put(Constants.PROFILER_KEY.METHOD_SIGNATURE, UtilsFactory.coverQuotes(strMethodSignature));
//					hmProfileCounter.put(Constants.PROFILER_KEY.CALLER_METHOD_ID, lCallerMethodId);
//					hmProfileCounter.put(Constants.PROFILER_KEY.CALLEE_METHOD_ID, getNextStackFunctionSerialId(strThreadSerialId));					
					
					htMethodDetails = new Hashtable<Constants.PROFILER_KEY, Object>();
					htMethodDetails.put(PROFILER_KEY.THREAD_ID, lUniqueThreadId);
					htMethodDetails.put(PROFILER_KEY.START_TIME, lStartTime);
					htMethodDetails.put(PROFILER_KEY.TICK_VALUE_MS, lTickValue);
					htMethodDetails.put(PROFILER_KEY.DURATION_MS, -1);
					htMethodDetails.put(PROFILER_KEY.CLASS_NAME, strClassName);
					htMethodDetails.put(PROFILER_KEY.METHOD_NAME, strMethodName);
					htMethodDetails.put(PROFILER_KEY.METHOD_SIGNATURE, "");
					htMethodDetails.put(PROFILER_KEY.CALLEE_METHOD_ID, lMethodSerialNo);
					htMethodDetails.put(PROFILER_KEY.CALLER_METHOD_ID, lCallerMethodId);
					
					if( lCallerMethodId == -1 ) {
						htMethodDetails.put(PROFILER_KEY.REQUEST_URI, strFunctionName);
					}
					
					stackUIDThreadStack.add(htMethodDetails);
					
					// Put all the HashTable and Stacks back in position
					htUIDThreads.put(strThreadId, stackUIDThreadStack);
					DOTNET_PROFILER_ALL_STACKTRACES.put(this.strUID, htUIDThreads);
					
					break;
				case "L":	// function execution is completed. So remove it from the virtual Stack.
					strThreadId = saMethodDetails[1];
					lTickValue = Long.parseLong(saMethodDetails[2]);
					strFunctionId = saMethodDetails[3];
					strEphochTime = saMethodDetails[4];	// Not used, as the Duration is calculated with TickValue
					
					// Get UID's all Thread Stacks 
					if( DOTNET_PROFILER_ALL_STACKTRACES.containsKey(this.strUID) ) {
						htUIDThreads = DOTNET_PROFILER_ALL_STACKTRACES.get(this.strUID);
						
						// Get Thread's Stack
						if( htUIDThreads.containsKey(strThreadId) ) {
							stackUIDThreadStack = htUIDThreads.get(strThreadId);
							
							// Get the Stack's Top Method. 
							htMethodDetails = stackUIDThreadStack.pop();
							
							lDuration = lTickValue - (Long)htMethodDetails.get(PROFILER_KEY.TICK_VALUE_MS);
							
							// Avoid 0ms, if set in Constants.MIN_ALLOWED_METHOD_EXECUTION_DURATION_MILLISEC
							if( lDuration >= Constants.MIN_ALLOWED_METHOD_EXECUTION_DURATION_MILLISEC ) {
								
								// TODO validate the Function Name, whether they are same; otherwise the Stack will be left with some elements always.
								
								// Subtract the Start-time, with the Time given now.
								// Add the time difference to the Hash Bean.
								htMethodDetails.put(PROFILER_KEY.DURATION_MS, lDuration);
								
								// Put the Object back into the Stack-DataStructure
								queueComputedMethod(strThreadId, htMethodDetails);
							}
							
							// Remove the UID, Thread & Stack references, if they are blank
							if( stackUIDThreadStack.size() == 0 ) {
								
								// Clear Stack if it is blank.
								UtilsFactory.clearCollectionHieracy( stackUIDThreadStack );
								
								// Remove the Thread reference once the Stack is empty.
								htUIDThreads.remove(strThreadId);
								
								// Remove the Thread-Id reference
								removeUniqueThreadIdReferene(this.strUID, strThreadId);
								
								// if the UID has no Threads then, destroy it. 
								synchronized ( DOTNET_PROFILER_ALL_STACKTRACES ) {
									if( htUIDThreads.size() == 0 ) {
										
										UtilsFactory.clearCollectionHieracy(htUIDThreads);
										
										DOTNET_PROFILER_ALL_STACKTRACES.remove(this.strUID);
									}
								}
							}
						} else {
							LogManager.errorLog("Unable to find the requested Thread in the ALL_STACKTRACES. UID: "+this.strUID+" <> Thread: "+strThreadId+" <> index: "+index+" <> Txt: "+strMethodDetails);
						}
					} else {
						LogManager.errorLog("Unable to find the requested UID in the ALL_STACKTRACES. UID: "+this.strUID+" <> index: "+index+" <> Txt: "+strMethodDetails);
					}
					
					
					break;
			}
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Load all the Function's Name-Id pairs, of the given UID, into memory.
	 * First clear the existing pairs.
	 */
	private void loadMethodReferences() {
		Date dateLog = LogManager.logMethodStart();
		
		Hashtable<String, String> htMethodNameIdPair = null;
		
		try{
			// Get the Hash of the Function name-Id pairs
			if( DOTNET_PROFILER_METHOD_REFERENCE.containsKey(this.strUID) ) {
				htMethodNameIdPair = DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID);
			} else {
				htMethodNameIdPair = new Hashtable<String, String>();
			}
			
			// Clear and load new Function Name-Id pairs
			synchronized ( htMethodNameIdPair ) {
				conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
				
				UtilsFactory.clearCollectionHieracy( htMethodNameIdPair );
				
				DOTNET_PROFILER_METHOD_REFERENCE.remove(this.strUID);
				
				// Load all the Function Name-Id pairs.
				htMethodNameIdPair = dotNetProfilerDBI.getMethodReferences(conDotNetProfiler, this.strUID);
				
				//System.out.println("Retrieved "+htMethodNameIdPair.size()+" Function Name-ID pairs, for UID: "+this.strUID);
				LogManager.infoLog("DNP <> Retrieved "+htMethodNameIdPair.size()+" Function Name-ID pairs, for UID: "+this.strUID);
				
				DOTNET_PROFILER_METHOD_REFERENCE.put(this.strUID, htMethodNameIdPair);
			}
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Add the Method Name-Id pair into memory.
	 * 
	 */
	private void addMethodReference(String strFunctionId, String strFunctionName) {
		Date dateLog = LogManager.logMethodStart();
		
		Hashtable<String, String> htMethodNameIdPair = null;
		
		try{	
			htMethodNameIdPair = DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID);
			
			synchronized ( htMethodNameIdPair ) {
				conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
				
				htMethodNameIdPair.put(strFunctionId, strFunctionName);
				
				// write the Function's Name-Id pairs into DB. On first request need to verify the DB and populate.
				dotNetProfilerDBI.addMethodReference(conDotNetProfiler, this.strUID, strFunctionId, strFunctionName);
				
				DOTNET_PROFILER_METHOD_REFERENCE.put(this.strUID, htMethodNameIdPair);
			}
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Remove the Method Name-Id pair from memory.
	 */
	public void removeMethodReference(String strFunctionId) {
		Date dateLog = LogManager.logMethodStart();
		
		Hashtable<String, String> htMethodNameIdPair = null;
		
		try{
			// Clear the values first
			if( DOTNET_PROFILER_METHOD_REFERENCE.containsKey(this.strUID) ) {
				synchronized ( htMethodNameIdPair ) {
					conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
					
					htMethodNameIdPair = DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID);
					
					// Delete the Function's Name-Id pairs from DB.
					dotNetProfilerDBI.deleteMethodReference(conDotNetProfiler, this.strUID, strFunctionId);
					
					// remove the key
					htMethodNameIdPair.remove(strFunctionId);
					
					// remove the UID itself, if the Map is empty
					if( htMethodNameIdPair.size() == 0 ) {
						UtilsFactory.clearCollectionHieracy( htMethodNameIdPair );
						UtilsFactory.clearCollectionHieracy( DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID) );
						DOTNET_PROFILER_METHOD_REFERENCE.remove(this.strUID);
					}
				}
			}
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Get the MethodName from Hash references.
	 * 
	 * @param strFunctionId
	 * @return
	 */
	private String getMethodReference(String strFunctionId) {
		Date dateLog = LogManager.logMethodStart();
		
		String strFunctionName = null;
		
		Hashtable<String, String> htMethodNameIdPair = null;
		
		// Get the Hash of the Methods
		if( DOTNET_PROFILER_METHOD_REFERENCE.containsKey(this.strUID) ) {
			htMethodNameIdPair = DOTNET_PROFILER_METHOD_REFERENCE.get(this.strUID);
			
			strFunctionName = htMethodNameIdPair.get(strFunctionId);
		}
		
		LogManager.logMethodEnd(dateLog);
		
		return strFunctionName;
	}
	
	/**
	 * Get the unique ThreadId for the given ThreadPool-ThreadId.
	 * If not available then get a new Thread-Id.
	 * 
	 * @param con
	 * @param strUID
	 * @param strThreadPool_ThreadId
	 * @return
	 */
	private static long getUniqueThreadId(Connection con, String strUID, String strThreadPool_ThreadId) {
		long lThreadId = 0l;
		Hashtable<String, Long> htThreadIdReference = null;
		
		// Get the UID's reference
		if( DOTNET_PROFILER_THREAD_SERIALS.containsKey(strUID) ) {
			htThreadIdReference = DOTNET_PROFILER_THREAD_SERIALS.get(strUID);
		} else {
			htThreadIdReference = new Hashtable<String, Long>();
		}
		
		// Get the Thread's reference
		if( htThreadIdReference.containsKey(strThreadPool_ThreadId) ) {
			lThreadId = htThreadIdReference.get(strThreadPool_ThreadId);
		} else {
			lThreadId = getNextUniqueThreadId(con, strUID, strThreadPool_ThreadId);
		}
		
		// put the values back in the Data-Structure
		htThreadIdReference.put(strThreadPool_ThreadId, lThreadId);
		DOTNET_PROFILER_THREAD_SERIALS.put(strUID, htThreadIdReference);
		
		return lThreadId;
	}
	
	/**
	 * If a new Stack is started then, get a Thread-Id.
	 * Check in DB for last used number. Or start from 1.
	 * 
	 * @param con
	 * @param strUID
	 * @param strThreadId
	 * @return
	 */
	private static long getNextUniqueThreadId(Connection con, String strUID, String strThreadId) {
		Long lNextThreadId = 1l;
		
		if( DOTNET_PROFILER_THREAD_LAST_SERIAL.containsKey(strUID) ) {
			synchronized ( DOTNET_PROFILER_THREAD_LAST_SERIAL.get(strUID) ) {
				lNextThreadId = DOTNET_PROFILER_THREAD_LAST_SERIAL.get(strUID);
				lNextThreadId++;
				DOTNET_PROFILER_THREAD_LAST_SERIAL.put(strUID, lNextThreadId);
			}
		} else {
			DOTNET_PROFILER_THREAD_LAST_SERIAL.put(strUID, 1l);	// insert a (temp) value, just to start synchronized block
			
			synchronized ( DOTNET_PROFILER_THREAD_LAST_SERIAL.get(strUID) ) {
				lNextThreadId = UtilsFactory.replaceNull( ((new DotNetProfilerDBI()).getLastUniqueThreadIdAssigned(con, strUID, strThreadId)), 1l);
				DOTNET_PROFILER_THREAD_LAST_SERIAL.put(strUID, lNextThreadId);
			}
		}
		
		return lNextThreadId;
	}
	
	/**
	 * Remove the Thread's Serial entry.
	 * If the Stack starts again then, new entry will be added with new Serial number.
	 * 
	 * @param strUID
	 * @param strThreadId
	 */
	private static void removeUniqueThreadIdReferene(String strUID, String strThreadId) {
		Hashtable<String, Long> htThreadIdReference = DOTNET_PROFILER_THREAD_SERIALS.get(strUID);
		htThreadIdReference.remove(strThreadId);
	}
	
	/**
	 * Get the next Serial number for the Method call happened in the given Thread.
	 * 
	 * @param strThreadId
	 * @return
	 */
	public int getNextMethodSerialNo(String strThreadId) {
		Date dateLog = LogManager.logMethodStart();
		
		int nSerialNumber = 0;
		Hashtable<String, Integer> htStack = null;
		
		if( DOTNET_PROFILER_STACK_METHOD_SERIALS.containsKey(this.strUID) ) {
			htStack = DOTNET_PROFILER_STACK_METHOD_SERIALS.get(this.strUID);
		} else {
			htStack = new Hashtable<String, Integer>();
		}
		
		if( htStack.containsKey(strThreadId) ) {
			nSerialNumber = htStack.get(strThreadId);
		} // else initialized 1 will be returned
		
		nSerialNumber++;
		htStack.put(strThreadId, nSerialNumber);
		
		DOTNET_PROFILER_STACK_METHOD_SERIALS.put(this.strUID, htStack);
		
		LogManager.logMethodEnd(dateLog);
		
		return nSerialNumber;
	}
	
	/**
	 * Once the Method's endTime reached & Duration is calculated then queue the Method entry.
	 * Another Thread will insert these in batch into DB.
	 * 
	 * @param strThreadId
	 * @param htMethodDetails
	 */
	public void queueComputedMethod(String strThreadId, Hashtable<Constants.PROFILER_KEY, Object> htMethodDetails) {
		Date dateLog = LogManager.logMethodStart();
		
		LinkedBlockingQueue< Hashtable<Constants.PROFILER_KEY, Object> > pbqMethods = null;
		
		if( DOTNET_PROFILER_COMPUTED_METHODS.containsKey(this.strUID) ) {
			pbqMethods = DOTNET_PROFILER_COMPUTED_METHODS.get(this.strUID);
		} else {
			pbqMethods = new LinkedBlockingQueue< Hashtable<Constants.PROFILER_KEY, Object> >();
		}
		
		synchronized ( pbqMethods ) {
			pbqMethods.add(htMethodDetails);
		}
		
		DOTNET_PROFILER_COMPUTED_METHODS.put(this.strUID, pbqMethods);
		DOTNET_PROFILER_COMPUTED_METHODS_UID.add(this.strUID);
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Get the computed Method Traces, which are ready to be inserted in DB.
	 * 
	 * @return
	 */
	public ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > getComputedGUID() {
		Date dateLog = LogManager.logMethodStart();
		
		LinkedBlockingQueue< Hashtable<Constants.PROFILER_KEY, Object> > pbqMethodTraces = null;
		ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > alComputedMethodTraces = new ArrayList< Hashtable<Constants.PROFILER_KEY, Object> >();
		
		if( (this.strUID = DOTNET_PROFILER_COMPUTED_METHODS_UID.poll()) != null ) {
			pbqMethodTraces = DOTNET_PROFILER_COMPUTED_METHODS.get(this.strUID);
			
			synchronized ( pbqMethodTraces ) {
				pbqMethodTraces.drainTo(alComputedMethodTraces);
			}
		}
		
		LogManager.logMethodEnd(dateLog);
		return alComputedMethodTraces;
	}
	
	/**
	 * Retrieves the profile-datasets from the respective queue.
	 * And inserts them in the database.
	 * 
	 * @throws Exception
	 */
	public void fetchData() throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		try{
			// if connection is not established to db server then wait for 10 seconds
			conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
			conDotNetProfiler.setAutoCommit( false );
			
			dotNetProfilerDBI.initializeInsertBatch(conDotNetProfiler);
			
			// take one-by-one counter from the queue; and it to the db-batch.
			// on reaching 100 counters in the batch or whn the queue is empty stop the loop.
			//System.out.println(".Net Profiler adding into batch...");
			addCounterBatch(this.alComputedMethodTraces);
			
			commitConnection();
			
			// once DBI work is over, close the Statements
			dotNetProfilerDBI.clearPreparedStatement();
			
		} catch(Throwable th) {
			LogManager.errorLog(th);
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Add the profile-dataset received to the queue, along with current time to keep track of received time.
	 * 
	 * @param strDotNetProfiler
	 * @throws Exception
	 */
	private void addCounterBatch(ArrayList< Hashtable<Constants.PROFILER_KEY, Object> > alMethodTraces) throws Exception {
		Date dateLog = LogManager.logMethodStart();
		
		Hashtable<Constants.PROFILER_KEY, Object> htMethodDetails = null;
		long lCallerId = -1, lCalleeId = -1;
		
		// loop through each profiler entry
		for( int i=0; i<alMethodTraces.size(); i++ ){
			conDotNetProfiler = DataBaseManager.reEstablishConnection(conDotNetProfiler);
			
			htMethodDetails = alMethodTraces.get(i);
			
			if( ! htMethodDetails.containsKey(PROFILER_KEY.REQUEST_URI) ){ htMethodDetails.put(PROFILER_KEY.REQUEST_URI, ""); }
			if( ! htMethodDetails.containsKey(PROFILER_KEY.CLASS_NAME) ){ htMethodDetails.put(PROFILER_KEY.CLASS_NAME, ""); }
			if( ! htMethodDetails.containsKey(PROFILER_KEY.METHOD_NAME) ){ htMethodDetails.put(PROFILER_KEY.METHOD_NAME, ""); }
			if( ! htMethodDetails.containsKey(PROFILER_KEY.METHOD_SIGNATURE) ){ htMethodDetails.put(PROFILER_KEY.METHOD_SIGNATURE, ""); }
			
			if( htMethodDetails.containsKey(PROFILER_KEY.CALLER_METHOD_ID) ){
				lCallerId = (Long)htMethodDetails.get(PROFILER_KEY.CALLER_METHOD_ID);
			} else {
				lCallerId = -1;
			}
			if( htMethodDetails.containsKey(PROFILER_KEY.CALLEE_METHOD_ID) ){
				lCalleeId = (Long)htMethodDetails.get(PROFILER_KEY.CALLEE_METHOD_ID);
			} else {
				lCalleeId = -1;
			}
			
			
			// add one-by-one profiler line into batch
			dotNetProfilerDBI.addCounterBatch(conDotNetProfiler, this.strUID, (Long)htMethodDetails.get(PROFILER_KEY.THREAD_ID), 
				(Long)htMethodDetails.get(PROFILER_KEY.START_TIME), 
				(Long)htMethodDetails.get(PROFILER_KEY.DURATION_MS), 
				(String)htMethodDetails.get(PROFILER_KEY.REQUEST_URI), 
				(String)htMethodDetails.get(PROFILER_KEY.CLASS_NAME), (String)htMethodDetails.get(PROFILER_KEY.METHOD_NAME), (String)htMethodDetails.get(PROFILER_KEY.METHOD_SIGNATURE), 
				lCallerId, lCalleeId
			);
			
			if( dotNetProfilerDBI.getBatchCount() > 3000 || i == alMethodTraces.size()-1 ){
				// insert the counter as batch
				executeCounterBatch(Thread.currentThread().getId());
			}
		}
		
		LogManager.logMethodEnd(dateLog);
	}
	
	/**
	 * Execute the batched counter-set inserts
	 * 
	 * @throws Exception
	 */
	private void executeCounterBatch(long lDistributorThreadId) throws Exception {
		dotNetProfilerDBI.executeCounterBatch(lDistributorThreadId);
	}
	
	/**
	 * Commit the DB-Connection, which is associated with this Object.
	 * 
	 * @throws Throwable
	 */
	public void commitConnection() throws Throwable {
		if( conDotNetProfiler != null ) {
			conDotNetProfiler.commit();
		}
	}
	
	/**
	 * Close the DB-Connection, which is associated with this Object.
	 */
	public void closeConnection() {
		DataBaseManager.close(conDotNetProfiler);
		conDotNetProfiler = null;
	}
	
	public static void main(String[] args) {
		System.out.println("<Unable to Profile>::".split("::")[1]);
	}
}
