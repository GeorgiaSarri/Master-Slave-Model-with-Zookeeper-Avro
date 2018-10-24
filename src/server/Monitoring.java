package org.thesis.project.server;

import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitoring {
	
	private final Logger monitorLog = LoggerFactory.getLogger(Monitoring.class.getName());
	
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	private final long SCHEDULER_DELAY = 10;
	
	private ConcurrentSkipListMap<Long, MonitoringElement> monitElmConcSkipList = new ConcurrentSkipListMap<Long, MonitoringElement>();
	
	protected AvroServer server;
	
	private static class MonitorHolder {
		static final Monitoring monitor = new Monitoring();
	}

	public static Monitoring getInstance() {
		return MonitorHolder.monitor;
	}
	
	public void init(AvroServer server) {
		this.server = server;
	}
	
	public void addMonitoring(MonitoringElement elem) {
		monitElmConcSkipList.put(elem.getElementId(), elem);
		
		Iterator<Long> it = monitElmConcSkipList.keySet().iterator();
		while (it.hasNext()) {
			Long keySet = (Long) it.next();
			monitorLog.info("[Monitor]: element-"+monitElmConcSkipList.get(keySet).getElementId()+ 
								" with status: "+ monitElmConcSkipList.get(keySet).getType()+
								" and duration: "+monitElmConcSkipList.get(keySet).getResponseTime());
		}		
	}
	
	protected ConcurrentSkipListMap<Long, MonitoringElement> copyAndClear() {
		
		ConcurrentSkipListMap<Long, MonitoringElement> copy = new ConcurrentSkipListMap<Long, MonitoringElement>();
		copy.putAll(monitElmConcSkipList);
		
		/*
			Iterator it = copy.keySet().iterator();
			while (it.hasNext()) {
				Long keySet = (Long) it.next();
				Log.info("[test]: Copy element-"+copy.get(keySet).getElementId()+ 
									" with status: "+ copy.get(keySet).getType()+
									" and duration: "+copy.get(keySet).getResponseTime());
			}
		*/
		
		monitElmConcSkipList.clear();
		return copy;
	}
	
	public ScheduledFuture<?> startMonitoring() {
		return scheduler.scheduleAtFixedRate(new MonitoringRunnable(this), SCHEDULER_DELAY, SCHEDULER_DELAY, TimeUnit.SECONDS);
	}
	
	public void stopMonitoring (ScheduledFuture<?> removeMonitor) {
		monitorLog.info("[Monitor]: Stop monitoring...");
		removeMonitor.cancel(true);
		scheduler.shutdown();
	}
}
