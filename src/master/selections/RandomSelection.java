package org.thesis.project.master.selections;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.master.ServerObj;

public class RandomSelection implements Selection {
	private static final Logger Log = LoggerFactory
			.getLogger(RandomSelection.class);
	
	private Random rnd = new Random();
		
	public String selectServer(ConcurrentSkipListMap<String, ServerObj> servers) {
		
		List<String> keys = new ArrayList<String>(servers.keySet());
		String randomServer = keys.get(rnd.nextInt(keys.size()));
		String ipAddress = servers.get(randomServer).getServerIP();

		while (ipAddress == "" || ipAddress == null) {
			Log.warn("ServerObj ip/id have not been initialized yet for key {}", randomServer);
			randomServer = keys.get(rnd.nextInt(keys.size()));
			ipAddress = servers.get(randomServer).getServerIP();
		}

		return randomServer;
	}

}
