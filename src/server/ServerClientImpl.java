package org.thesis.project.server;

import java.text.ParseException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.avro.rpc.protocol.Message;
import org.thesis.project.avro.rpc.protocol.ServerClient;
import org.thesis.project.avro.rpc.protocol.Status;

public class ServerClientImpl implements ServerClient {

	private static final Logger Log = LoggerFactory.getLogger(ServerClient.class);

	private final int SLEEP_DURATION = 20000;
	private final AvroServer server;

	private final Random rnd = new Random();

	private final Monitoring monitor;
	private final ScheduledFuture<?> schedMonitor;
	
	public ServerClientImpl(AvroServer server) {
		this.server = server;
		monitor = Monitoring.getInstance();
		monitor.init(server);
		schedMonitor = monitor.startMonitoring();
	}

	public CharSequence send(Message message) throws AvroRemoteException,
			ParseException, KeeperException, InterruptedException {
		Log.info("Method \"send\" has been invoked...");
		Log.info("clientID: " + message.clientID);
		Log.info("status: " + message.status);
		Log.info("timestamp: " + message.timestamp);
		Log.info("procedure data: " + message.data);

		String result;
		Long prevTimestamp;
		ConcurrentHashMap<String, ClientObj> clientsCopy = server.getClientsMap();
		ClientObj obj;
		//long runtime = Long.parseLong(message.runtime.toString());
		switch (Status.valueOf(message.status.toString())) {

		case INIT:
			result = initConnection(message);
			break;

		case SELECT:
			obj = clientsCopy.get(message.clientID.toString());
			if (obj == null){
				result = ExceptionCodes.ClientExpired.toString();
				break;
			}
			prevTimestamp = obj.getTimestamp();
			if (prevTimestamp != null && Long.valueOf(message.timestamp.toString()) > prevTimestamp) 
				result = doSelect(message);
			else {
				if (prevTimestamp == null)
					result = ExceptionCodes.ClientExpired.toString();
				else
					result = ExceptionCodes.RequestExpired.toString();
			}
			break;
		case UPDATE:
			obj = clientsCopy.get(message.clientID.toString());
			if (obj == null){
				result = ExceptionCodes.ClientExpired.toString();
				break;
			}
			prevTimestamp = obj.getTimestamp();
			if (prevTimestamp != null && Long.valueOf(message.timestamp.toString()) > prevTimestamp) 
				result = doUpdate(message);
			else {
				if (prevTimestamp == null)
					result = ExceptionCodes.ClientExpired.toString();
				else
					result = ExceptionCodes.RequestExpired.toString();
			}
			break;
		case INSERT:
			obj = clientsCopy.get(message.clientID.toString());
			if (obj == null){
				result = ExceptionCodes.ClientExpired.toString();
				break;
			}
			prevTimestamp = obj.getTimestamp();
			if (prevTimestamp != null && Long.valueOf(message.timestamp.toString()) > prevTimestamp) 
				result = doInsert(message);
			else {
				if (prevTimestamp == null)
					result = ExceptionCodes.ClientExpired.toString();
				else
					result = ExceptionCodes.RequestExpired.toString();
			}
			break;
		case DELETE:
			obj = clientsCopy.get(message.clientID.toString());
			if (obj == null){
				result = ExceptionCodes.ClientExpired.toString();
				break;
			}
			prevTimestamp = obj.getTimestamp();
			if (prevTimestamp != null && Long.valueOf(message.timestamp.toString()) > prevTimestamp) 
				result = doDelete(message);
			else {
				if (prevTimestamp == null)
					result = ExceptionCodes.ClientExpired.toString();
				else
					result = ExceptionCodes.RequestExpired.toString();
			}
			break;
		default:
			result = ExceptionCodes.StatusNotSpecified.toString();
			break;
		}

		return new Utf8(result);
	}

	public String initConnection(Message message) throws KeeperException,InterruptedException {
		
		long startTime = new Date().getTime();
		
		//long runtime = Long.parseLong(message.runtime.toString());
		ClientObj clientInfo = new ClientObj(message.clientID.toString(), message.status.toString(),message.timestamp.toString(), message.data.toString());

		synchronized (server) {
			server.addClient(clientInfo);
			server.incrSrvClients();
		}
		
		long endTime = new Date().getTime();
		MonitoringElement monitorElem = new MonitoringElement(message.status.toString(), endTime - startTime);
		monitor.addMonitoring(monitorElem);
		
		return "OK";
	}

	public String doSelect(Message message) throws KeeperException,InterruptedException {
		
		String clientId = message.clientID.toString();
		
		long startTime = new Date().getTime();
		
		//InterruptedException e = new InterruptedException();
		//throw e;
		
		int sleepTime = rnd.nextInt(SLEEP_DURATION);
		Log.info("Method \"doSelect\" starts sleeping for " + sleepTime + "...");
		Thread.sleep(sleepTime);
		Log.info("Method \"doSelect\" woke up...");
		String result;
		
		synchronized (server) {
			server.updateClientTimestamp(clientId, message.timestamp.toString());
			result = server.updateClDateLastModified(clientId, new Date());
		}
		
		long endTime = new Date().getTime();
		MonitoringElement monitorElem = new MonitoringElement(Status.SELECT.toString(), endTime - startTime);
		monitor.addMonitoring(monitorElem);

		return result;
	}

	public String doInsert(Message message) throws KeeperException,InterruptedException {

		String clientId = message.clientID.toString();
		
		long startTime = new Date().getTime();
		
		int sleepTime = rnd.nextInt(SLEEP_DURATION);
		Log.info("Method \"doInsert\" starts sleeping for " + sleepTime + "...");
		Thread.sleep(sleepTime);
		Log.info("Method \"doInsert\" woke up...");
		String result;
		
		synchronized (server) {
			server.updateClientTimestamp(clientId, message.timestamp.toString());
			result = server.updateClDateLastModified(clientId, new Date());
		}

		long endTime = new Date().getTime();
		MonitoringElement monitorElem = new MonitoringElement(Status.INSERT.toString(), endTime - startTime);
		monitor.addMonitoring(monitorElem);

		return result;
	}

	public String doUpdate(Message message) throws KeeperException,InterruptedException {
		
		String clientId = message.clientID.toString();
		
		long startTime = new Date().getTime();
		
		int sleepTime = rnd.nextInt(SLEEP_DURATION);
		Log.info("Method \"doUpdate\" starts sleeping for " + sleepTime + "...");
		Thread.sleep(sleepTime);
		Log.info("Method \"doUpdate\" woke up...");
		String result;
		
		synchronized (server) {
			server.updateClientTimestamp(clientId, message.timestamp.toString());
			result = server.updateClDateLastModified(clientId, new Date());
		}
		
		long endTime = new Date().getTime();
		MonitoringElement monitorElem = new MonitoringElement(Status.UPDATE.toString(), endTime - startTime);
		monitor.addMonitoring(monitorElem);

		return result;
		
	}

	public String doDelete(Message message) throws KeeperException,	InterruptedException {
		
		String clientId = message.clientID.toString();
		
		long startTime = new Date().getTime();
		
		int sleepTime = rnd.nextInt(SLEEP_DURATION);
		Log.info("Method \"doDelete\" starts sleeping for " + sleepTime + "...");
		Thread.sleep(sleepTime);
		Log.info("Method \"doDelete\" woke up...");
		String result;
		
		synchronized (server) {
			server.updateClientTimestamp(clientId, message.timestamp.toString());
			result = server.updateClDateLastModified(clientId, new Date());
		}
		
		long endTime = new Date().getTime();
		MonitoringElement monitorElem = new MonitoringElement(Status.DELETE.toString(), endTime - startTime);
		monitor.addMonitoring(monitorElem);

		return result;
	}


	public CharSequence bye(CharSequence clientID) throws AvroRemoteException,
			KeeperException, InterruptedException {
		Log.info("Method \"bye\" has been invoked...");
		Log.info("clientID: " + clientID);

		long totalRunTime = new Date().getTime()
				- server.getClient(clientID.toString()).getStartedTime()
						.getTime();

		synchronized (server) {	
			server.decrSrvClients();
			server.incrSrvTotalRuntime(totalRunTime);
			server.removeClient(clientID.toString());
		}
		return new Utf8("OK");
	}

	public void stopMonitoring() {
		monitor.stopMonitoring(schedMonitor);
	}
}
