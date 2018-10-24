package org.thesis.project.server;

import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringRunnable implements Runnable {

	private final Logger Log = LoggerFactory.getLogger(MonitoringRunnable.class.getName());
	
	private final Monitoring monitor;
	private final AvroServer server;
	
	//private double avgTotalRuntime = 0;
	
	public MonitoringRunnable(Monitoring monitor) {
		this.monitor = monitor;
		this.server = monitor.server;
	}
	
	public void run() {
		Log.info("[MonitoringRunnable]: Start monitoring...");
		
		ConcurrentSkipListMap<Long, MonitoringElement> copy = new ConcurrentSkipListMap<Long, MonitoringElement>();
		
		synchronized (monitor) {
			copy = monitor.copyAndClear();
		}
		double tempAvg = 0.0;
		Iterator<Long> it = copy.keySet().iterator();
		while (it.hasNext()) {
			Long keySet = (Long) it.next();
			Log.info("[MonitoringRunnable]: Element-"+copy.get(keySet).getElementId()+ 
								" with status: "+ copy.get(keySet).getType()+
								" and duration: "+copy.get(keySet).getResponseTime());
			tempAvg += copy.get(keySet).getResponseTime();
		}
		if (copy.size() == 0)
			return;
		else
			tempAvg /= copy.size();
		
		//avgTotalRuntime += tempAvg;
		
		//Log.info("[MonitoringRunnable]: Avg = " + avgTotalRuntime);
		Log.info("[MonitoringRunnable]: SrvAvg = " + server.getsrvUpAvgTime());
		Log.info("[MonitoringRunnable]: Time Added = " + tempAvg);
		
		synchronized (server) {
			try {
				server.updateSrvUpAvgTime(tempAvg);
			} catch (KeeperException e) {
				Log.error("[MonitoringRunnable]" + e.getMessage());
			} catch (InterruptedException e) {
				Log.error("[MonitoringRunnable]" + e.getMessage());
			}
		}


		
		/*String path = "/srvUpAvgTime/"+server.getName();
		try {
			zk.setData(path, String.valueOf(avgTotalRuntime).getBytes(), zk.exists(path, true).getVersion());
		} catch (KeeperException e) {
			Log.error("[MonitoringRunnable]" + e.getMessage());
		} catch (InterruptedException e) {
			Log.error("[MonitoringRunnable]" + e.getMessage());
		}*/
	}

}
