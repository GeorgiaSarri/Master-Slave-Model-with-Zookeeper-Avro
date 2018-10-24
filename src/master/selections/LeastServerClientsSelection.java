package org.thesis.project.master.selections;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.thesis.project.master.ServerObj;

public class LeastServerClientsSelection implements Selection {

	public String selectServer(ConcurrentSkipListMap<String, ServerObj> serversMap) {
		
		Iterator<Entry<String, ServerObj>> it = serversMap.entrySet().iterator();
		
		ServerObj result = null;
		
		while (it.hasNext()) {
			
			if(result == null) {
				result = it.next().getValue();
				continue;
			}
			
			Entry<String, ServerObj> entry = it.next();
			//String keySet = entry.getKey();
			ServerObj obj = entry.getValue();
						
			if (obj.getSrvClients() < result.getSrvClients())
					result = obj;
		}
			
		return result.getServerID();
	}

}
