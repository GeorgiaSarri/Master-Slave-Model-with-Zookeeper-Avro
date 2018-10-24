package org.thesis.project.client;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.client.exceptions.ClientServerException;
import org.thesis.project.client.exceptions.UserExpiredException;

public class User {

	private static final Logger UserLog = LoggerFactory.getLogger(User.class);
	
	public static Client createClient() {
		Client client = Client.getInstance();
		
		return client;
	}
	
	public static void createConnection(Client client) throws ClientServerException {
		client.newConnection();
	}
	
	public static void main(String[] args) throws InterruptedException, IOException, KeeperException, UserExpiredException, ClientServerException{
		
		Client c = createClient();
		
		createConnection(c);

		String resSelect = c.doSelect();
		UserLog.info("[User]: Method \"doSelect\" was invoked... Result : " + resSelect);

		//UserLog.info("Starts sleeping...");
		//Thread.sleep(210000);
		String resUpdate = c.doUpdate();
		UserLog.info("[User]: Method \"doUpdate\" was invoked... Result : " + resUpdate);
		
		String resInsert = c.doInsert();
		UserLog.info("[User]: Method \"doInsert\" was invoked... Result : " + resInsert);

		String resDelete = c.doDelete();
		UserLog.info("[User]: Method \"doDelete\" was invoked... Result : " + resDelete);

		c.stopClient();
		
		
	}
}
