package org.thesis.project.server;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.avro.rpc.protocol.ServerClient;

/**
 *The server connects with the Zookeeper to store its average and total running time </br>
 *as well as the number of clients he is responsible for. It also connects with the clients through </br>
 *Avro. It has two schedulers attached το it; one to remove expired clients and one to calculate the </br>
 *average time that it's running.
 * 
 * @author Georgia Sarri
 *
 */
public class AvroServer implements Closeable, Watcher{
	
	private static final Logger serverLog = LoggerFactory.getLogger(AvroServer.class);
	
	private final static int NUM_OF_SERVERS = 1;
	
	private final ServerClientImpl servClieImpl = new ServerClientImpl(this);
	
	private static ZooKeeper zk;
	private String host;
	private final String serverId = UUID.randomUUID().toString();
	private final String name = "server-"+serverId;
	
	private volatile double srvUpAvgTime = 0.0;
	private volatile int  srvClients = 0;
	private volatile long srvTotalRuntime = 0;
	
	private volatile boolean connected = false;
	private volatile boolean expired = false;
	
	
	private InetSocketAddress addr;
	private static Server server;
	
	private ConcurrentHashMap<String, ClientObj> clientsMap = new ConcurrentHashMap<String, ClientObj>();

	final ClientsControl clientsControl;
	final ScheduledFuture<?> expiredClRemover;
		
	public AvroServer(String host){
		this.host = host;
		clientsControl = ClientsControl.getInstance();
		clientsControl.init(this);
		expiredClRemover = clientsControl.removeExpiredClients();
		
	}
	
	
	public void startServer(){
		serverLog.info("Server is starting...");
		addr = new InetSocketAddress("localhost", 0);
		server = new NettyServer(new SpecificResponder(ServerClient.class, servClieImpl), addr);
		serverLog.info("Server listens to: " + server.getPort());
		server.start();
		
		
	}
	public void stopServer() throws IOException{
		serverLog.info("Server is stoping...");
		
		servClieImpl.stopMonitoring();
		clientsControl.stopClientsControl(expiredClRemover);
		
		if(server!=null)
			server.close();
		close();
	}
	
	public synchronized ConcurrentHashMap<String, ClientObj> getClientsMap() {
		return clientsMap;
	}
	
	public String getName() {
		return name;
	}

	public synchronized double getsrvUpAvgTime() {
		return srvUpAvgTime;
	}

	public synchronized void setsrvUpAvgTime(double srvUpAvgTime) {
		this.srvUpAvgTime = srvUpAvgTime;
	}

	public synchronized long getSrvTotalRuntime() {
		return srvTotalRuntime;
	}

	public synchronized void setSrvTotalRuntime(long srvTotalRuntime) {
		this.srvTotalRuntime = srvTotalRuntime;
	}

	public synchronized int getSrvClients() {
		return srvClients;
	}

	public synchronized void setSrvClients(int srvClients) {
		this.srvClients = srvClients;
	}

	public String getServerId() {
		return serverId;
	}

	/**
	 * Creates a Zookeeper session.
	 * 
	 * @throws IOException
	 */
	public void startZk() throws IOException {
		zk = new ZooKeeper(host, 15000, this);
	}
	
	/**
	 * Deals with session events like connecting and disconnecting
	 */
	public void process(WatchedEvent e) {
		serverLog.info(e.toString() + ", "+host);
		if(e.getType()== Event.EventType.None) {
			switch (e.getState()){
			case SyncConnected:
				connected = true;
				break;
			case Disconnected:
				connected = false;
				break;
			case Expired:
				expired = true;
				connected = false;
				serverLog.error("Session Expired...");
				break;
			default:
				break;
			}
		}
	}
	
	/**
	 * Checks if server is connected to Zookeeper.
	 * 
	 * @return boolean
	 */
	public boolean isConnected() {
		return connected;
	}

	/**
	 * Checks if session is expired
	 * 
	 * @return boolean
	 */
	public boolean isExpired() {
		return expired;
	}
	
	/**
	 * Bootstraping here is just creating a /connections parent
	 * znode to hold the clients connections to this server.
	 */
	public void bootstrap() {
		createConnectionsNode();
	}
	
	void createConnectionsNode() {
		zk.create("/connections/server-"+serverId, 
				(addr.getAddress()+":"+server.getPort()).toString().getBytes(), 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT,
					createConnectionsCallback,
					null);
	}
	
	StringCallback createConnectionsCallback = new StringCallback() {

		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				createConnectionsNode();
				break;
			case OK:
				serverLog.info("connections node created...");
				break;
			case NODEEXISTS:
				serverLog.warn("connections node already registered");
				break;
			default:
				serverLog.error("Something went wrong...", KeeperException.create(Code.get(rc),path));
			}
		}
	};
	
	/**
	 * Registering the new server, which consists of adding a new 
	 * server to /servers.
	 */
	public void register() {
		zk.create("/servers/"+name,
					(addr.getAddress()+":"+server.getPort()).toString().getBytes(), 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL,
					createServerCallback,
					null);
		registersrvUpAvgTime();
		registerSrvTotalRuntime();
		registerSrvClients();
	}
	
	StringCallback createServerCallback = new StringCallback() {
		public void processResult (int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				register();
				break;
			case OK:
				serverLog.info("Registered successfully..."+serverId);
				break;
			case NODEEXISTS:
				serverLog.warn("Already registered..."+serverId);
				break;
			default:
				serverLog.error("Something went wrong"+KeeperException.create(Code.get(rc),path));
			}
		}
	};
	
	public void registersrvUpAvgTime() {
		//!!! Using different znodes instead of creating children /servers/serverID/srvUpAvgTime 
		//because ephemeral znodes cannot have children!
		zk.create("/srvUpAvgTime/"+name, 
					"0".getBytes(), 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL,
					createsrvUpAvgTimeCallback,
					null);
	}
	
	StringCallback createsrvUpAvgTimeCallback = new StringCallback() {
		public void processResult (int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				registersrvUpAvgTime();
				break;
			case OK:
				serverLog.info("Server up time node created successfully..."+serverId);
				break;
			case NODEEXISTS:
				serverLog.warn("Up time node already exists..."+serverId);
				break;
			default:
				serverLog.error("Something went wrong"+KeeperException.create(Code.get(rc),path));
			}
		}
	};
   
	public void registerSrvTotalRuntime() {
		zk.create("/srvTotalRuntime/"+name, 
				"0".getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL,
				createSrvTotalRuntimeCallback,
				null);
	}
	
	StringCallback createSrvTotalRuntimeCallback = new StringCallback() {
		public void processResult (int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				registerSrvTotalRuntime();
				break;
			case OK:
				serverLog.info("Server runtime node created successfully..."+serverId);
				break;
			case NODEEXISTS:
				serverLog.warn("Runtime node already exists..."+serverId);
				break;
			default:
				serverLog.error("Something went wrong"+KeeperException.create(Code.get(rc),path));
			}
		}
	};
	
	public void registerSrvClients() {
		zk.create("/srvClients/"+name, 
					"0".getBytes(), 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL,
					createSrvClientsCallback,
					null);
	}
	
	StringCallback createSrvClientsCallback = new StringCallback() {
		public void processResult (int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				registerSrvClients();
				break;
			case OK:
				serverLog.info("Server clients node created successfully..."+serverId);
				break;
			case NODEEXISTS:
				serverLog.warn("Clients node already exists..."+serverId);
				break;
			default:
				serverLog.error("Something went wrong"+KeeperException.create(Code.get(rc),path));
			}
		}
	};
	
	
	public void incrSrvTotalRuntime(long time) throws KeeperException, InterruptedException {
		String path = "/srvTotalRuntime/"+name;
		srvTotalRuntime += time;
		String timeStr = String.valueOf(srvTotalRuntime);
		zk.setData(path, timeStr.getBytes(), zk.exists(path, true).getVersion());
	}
	
	public void updateSrvUpAvgTime(double time) throws KeeperException, InterruptedException {
		String path = "/srvUpAvgTime/"+name;
		srvUpAvgTime += time;
		String timeStr = String.valueOf(srvUpAvgTime);
		serverLog.info("[Server]: Updating " + path + "srvUpAvgTime = " + timeStr);
		zk.setData(path, timeStr.getBytes(), zk.exists(path, true).getVersion());
		serverLog.info("[Server]: Path "+ path + " Updated!!!");
	  }
	
	public void incrSrvClients() throws KeeperException, InterruptedException {
		String path = "/srvClients/"+name;
		srvClients++;
		String srvClientsStr = String.valueOf(srvClients);
		serverLog.info("[Server]: --Clients-- {} "+ srvClientsStr);
		System.out.println(path);
		zk.setData(path, srvClientsStr.getBytes(), zk.exists(path, true).getVersion());
	}
	
	public void decrSrvClients() throws KeeperException, InterruptedException {
		String path = "/srvClients/"+name;
		srvClients--;
		String srvClientsStr = String.valueOf(srvClients);
		serverLog.info("[Server]: --Clients-- {} "+ srvClientsStr);
		System.out.println(path);
		zk.setData(path, srvClientsStr.getBytes(), zk.exists(path, true).getVersion());
	}
	
    /**
     * Close Zookeeper session
     */
	public void close() throws IOException {
		serverLog.info("Closing session");
		try {
			zk.close();
		}catch (InterruptedException e) {
			serverLog.warn("Zookeeper interrupted while closing");
		}
	}

	/**
	 * Returns a client from the hash map
	 * 
	 * @param clientId
	 */
	public ClientObj getClient(String clientId) {
		ClientObj client = clientsMap.get(clientId);
		return client;
	}
	/**
	 * Adds client to hash map
	 * 
	 * @param ClientObj
	 */
	public void addClient(ClientObj client) {
		
		serverLog.info("[Server]: Adding client "+ client.getClientID() + " to map...");
		
		String clientKey = client.getClientID();
		clientsMap.putIfAbsent(clientKey, client);
		
		serverLog.info("==== CLIENTS ====");
		Iterator<String> it = clientsMap.keySet().iterator();
		while (it.hasNext()) {
			String keySet = (String) it.next();
			serverLog.info(" Key: " + keySet + " IP: "
					+ clientsMap.get(keySet).getClientID() + " Clients: "
					+ clientsMap.get(keySet).getStatus());
		}
		serverLog.info("=============");
	}
	
	/**
	 * Removes client from hash map
	 * 
	 * @param ClientObj
	 */
	public void removeClient(String clientKey) {
		serverLog.info("[Server]: Removing client "+ clientKey + " from map...");

		clientsMap.remove(clientKey);
		
		serverLog.info("==== CLIENTS ====");
		Iterator<String> it = clientsMap.keySet().iterator();
		while (it.hasNext()) {
			String keySet = (String) it.next();
			serverLog.info(" Key: " + keySet + " IP: "
					+ clientsMap.get(keySet).getClientID() + " Clients: "
					+ clientsMap.get(keySet).getStatus());
		}
		serverLog.info("=============");
	}
	
	/**
	 * Updates client's Timestamp
	 * 
	 * @param String clientId, String timestamp
	 */
	public void updateClientTimestamp(String clientId, String timestampStr) {
		
		long timestamp= Long.valueOf(timestampStr);
		
		ClientObj client = clientsMap.get(clientId);
		
		if (client != null)
			client.setTimestamp(timestamp);
	}
	
	/**
	 * Updates client's DateLastModified
	 * 
	 * @param String clientId, Date newDate
	 */
	public String updateClDateLastModified(String clientId, Date newDate) {
		ClientObj client = clientsMap.get(clientId);
		if (client == null )
			return ExceptionCodes.ClientExpired.toString();
		
		else {
			client.setDateLastModified(newDate);
			serverLog.info("[Server]: Client-" + clientId + " last active on :" + newDate);
			return "OK";
		}
	}
	
	public static void main(String args[]) throws Exception { 
       
		ArrayList<AvroServer> servers = new ArrayList<AvroServer>();
		
		for (int i = 0; i < NUM_OF_SERVERS; i++) {
			AvroServer w = new AvroServer(args[0]);

			w.startZk();
			w.startServer();
			
			while (!w.isConnected()) {
				Thread.sleep(100);
			}
			/*
			 * bootstrap() create some necessary znodes.
			 */
			w.bootstrap();

			/*
			 * Registers this server so that the leader knows that it is here.
			 */
			w.register();
			
			servers.add(w);

			Thread.sleep(10000);
		} 
		
		/*Thread.sleep(10000);
		
		for(int i = 0; i<servers.size(); i++) {
			servers.get(i).stopServer();
			Thread.sleep(20000);
		}*/
    }
}
