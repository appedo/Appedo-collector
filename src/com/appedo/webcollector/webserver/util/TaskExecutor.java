package com.appedo.webcollector.webserver.util;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.appedo.manager.LogManager;

public class TaskExecutor
{

	private static ConcurrentHashMap<String, TaskExecutor> map = new ConcurrentHashMap<String, TaskExecutor>();
	private ThreadPoolExecutor executor;
	private AtomicLong counter = new AtomicLong();

	//Map<String,String> m = new HashMap<String,String>();
	private TaskExecutor(final String name, int minthreadPoolSize, int maxthreadPoolSize, int queueSize)
	{
		BlockingQueue<Runnable> queue = null;
		ThreadFactory tf = new ThreadFactory()
		{
			public Thread newThread(Runnable run)
			{
				return new Thread(run, name + "-" + counter.getAndIncrement());
			}
		};
		//Below condition to allow default queue size when not providing queue size
		if (queueSize > 0) {
			queue = new LinkedBlockingQueue<Runnable>(queueSize);
		} else {
			queue = new LinkedBlockingQueue<Runnable>();
		}
		
		executor = new ThreadPoolExecutor(minthreadPoolSize, maxthreadPoolSize, 1, TimeUnit.MINUTES, queue, tf);
		executor.allowCoreThreadTimeOut(true);
		//m.put("kkk","test");
	}

	public static void newExecutor(String name, int minthreadPoolSize, int maxthreadPoolSize, int queueSize)throws Exception
	{
		if (minthreadPoolSize <= 0)
		{	 
			throw new IllegalArgumentException("Min ThreadPoolSize must be greater than 0");
		}
		if (maxthreadPoolSize <= 0)
		{	 
			throw new IllegalArgumentException("Max ThreadPoolSize must be greater than 0");
		}
		//commented below condition to allow default queue size
		if (queueSize < 0 ) 
		{
			throw new IllegalArgumentException("Internal Queue Size must be greater 0");
		}
//		if (handler == null) 
//		{
//			throw new IllegalArgumentException("RejectedExecutionHandler must not be NULL");
//		}

		TaskExecutor executor = new TaskExecutor(name,minthreadPoolSize,maxthreadPoolSize,queueSize);
		map.put(name,executor);
	}

	public static TaskExecutor getExecutor(String name)throws Exception
	{
		TaskExecutor executor = map.get(name);
		if(executor==null)
		{
			throw new IllegalArgumentException("TaskExecutor '"+name +"' not found");
		}
		return executor;
	}

	public void submit(Runnable runnable) throws Exception
	{
		executor.execute(runnable);
		//LogManager.infoLog("No. of Threads in Use: "+executor.getActiveCount() +" getLargestPoolSize() :" +executor.getLargestPoolSize());
	}
	
	public long activeThreadCount() throws Exception{
		return executor.getActiveCount();
	}
}
