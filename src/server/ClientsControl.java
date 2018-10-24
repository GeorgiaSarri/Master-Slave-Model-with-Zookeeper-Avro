package org.thesis.project.server;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientsControl {
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	private final Logger schLog = LoggerFactory.getLogger(ClientsControl.class.getName()); 
	private AvroServer server;
	
	private final long EXPIRATION_LIMIT = 200000;
	private final long SCHEDULER_DELAY = 10;
	
	private static class ClientsControlHolder {
		static final ClientsControl controler = new ClientsControl();
	}

	public static ClientsControl getInstance() {
		return ClientsControlHolder.controler;
	}
	
	public void init(AvroServer server) {
		this.server = server;
	}

	public ScheduledFuture<?> removeExpiredClients() {
		final Runnable remover = new Runnable() {
			
			public void run() {
				schLog.info("[ClientsControl]: Expired Clients remover is starting...");
				ConcurrentHashMap<String, ClientObj> clientsCopy = server.getClientsMap();
				long curTime = new Date().getTime();
				Iterator<String> it = clientsCopy.keySet().iterator();
				while (it.hasNext()) {
					String keySet = (String) it.next();
					long clDateLastModified = clientsCopy.get(keySet).getDateLastModified().getTime();
					if ( curTime - clDateLastModified > EXPIRATION_LIMIT )
						synchronized (keySet) {
							//server.clientsMap.remove(keySet);
							server.removeClient(keySet);
							try {
								server.decrSrvClients();
								
							} catch (KeeperException e) {
								schLog.info("[ClientsControl]:" + e.getMessage());
							} catch (InterruptedException e) {
								schLog.info("[ClientsControl]:" + e.getMessage());
							}
						}
				}				
			}
		};
		
		//final ScheduledFuture<?> removerHandle = scheduler.scheduleAtFixedRate(remover, SCHEDULER_DELAY, SCHEDULER_DELAY, TimeUnit.SECONDS);
		return scheduler.scheduleAtFixedRate(remover, SCHEDULER_DELAY, SCHEDULER_DELAY, TimeUnit.SECONDS);
	}
	
	public void stopClientsControl(ScheduledFuture<?> remover) {
		remover.cancel(true);
		scheduler.shutdown();
	}
}
