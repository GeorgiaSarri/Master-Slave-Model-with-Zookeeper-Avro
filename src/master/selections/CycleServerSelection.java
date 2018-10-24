package org.thesis.project.master.selections;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.thesis.project.master.ServerObj;

public class CycleServerSelection implements Selection {

	private static volatile int counter;

	public String selectServer(	ConcurrentSkipListMap<String, ServerObj> serversMap) {
		String designatedServer;

		counter++;
	
		Iterator<String> it = serversMap.keySet().iterator();
		
		if (counter == 0) {
			String keySet = it.next();
			designatedServer = serversMap.get(keySet).getServerID();
		} else {
			int i = 0;
			String keySet = null;
			while (it.hasNext() && i < counter) {
				keySet = it.next();
				i++;
			}
			designatedServer = serversMap.get(keySet).getServerID();
		}
		
		if (counter == serversMap.size())
			counter = 0;
		System.out.println("SELECT "+ designatedServer);
		return designatedServer;
	}

}
