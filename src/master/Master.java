package org.thesis.project.master;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.avro.rpc.protocol.MasterClient;
import org.thesis.project.avro.rpc.protocol.MasterResp;
import org.thesis.project.master.selections.CycleServerSelection;
import org.thesis.project.master.selections.LeastServerClientsSelection;
import org.thesis.project.master.selections.LeastServerTotalRuntimeSelection;
import org.thesis.project.master.selections.LeastServerUpTimeSelection;
import org.thesis.project.master.selections.RandomSelection;
import org.thesis.project.master.selections.Selection;
import org.thesis.project.server.AvroServer;

/**
 * The Master is responsible to create the required znodes (/master, /servers,
 * /connections) in ZooKeeper, accept a client using MasterClient Protocol and
 * assign said client to a server.</br> It can only exist one master at a time,
 * the one that will create the /master znode first.</br></br> It holds all
 * servers in a concurrent hashMap which has as a key the server's name and a
 * ServerObj with some info for said server.</br> The hashMap is updated via
 * some extra lists the addedServers and deletedServers which holds the newly
 * added servers in the /servers znode and the removed one respectively. The
 * ones in the first list are added to the hashMap and the second ones are
 * removed. </br></br>
 * 
 * @author Georgia Sarri
 *
 */

public class Master implements Watcher, Closeable {
	private static final Logger masterLog = LoggerFactory.getLogger(Master.class);

	private static Server master;
	private static final InetSocketAddress masterAddress = new InetSocketAddress(8081);

	private final String masterId = UUID.randomUUID().toString();

	private static ZooKeeper zk;
	private String host = "localhost";

	private volatile boolean connected = false;
	private volatile boolean expired = false;

	private static ConcurrentSkipListMap<String, ServerObj> serversMap = new ConcurrentSkipListMap<String, ServerObj>();
	protected static ChildrenList serversList;
	
	protected static CopyOnWriteArrayList<String> clToAssignList = new CopyOnWriteArrayList<String>();
	protected static CopyOnWriteArrayList<String> clientsList = new CopyOnWriteArrayList<String>();
	
	/*
	 * ========================================================================
	 * ==================== SERVER PART OF MASTER ============================
	 * =========================================================================
	 */
	public void startMaster() {
		masterLog.info("Starting Master-" + masterId + "...");
		master = new NettyServer(new SpecificResponder(MasterClient.class,
				new MasterClientImpl(this)), masterAddress);
		masterLog.info("Master listens to : " + master.getPort());
		master.start();
	}

	public void stopMaster() {
		masterLog.info("Master is stoping...");
		if (master != null)
			master.close();
	}

	/*
	 * ========================================================================
	 * =========================================================================
	 * 
	 * ==========================================================================
	 * ================== ZOOKEEPER PART OF MASTER ===========================
	 * =========================================================================
	 */

	/*
	 * enums about the state of a master : RUNNING = running for primary master,
	 * ELECTED = is primary master, NOTELECTED = is a backup master.
	 */
	enum MasterStates {
		RUNNING, ELECTED, NOTELECTED
	};

	private volatile MasterStates state = MasterStates.RUNNING;

	public MasterStates getState() {
		return state;
	}

	/**
	 * Master Constructor
	 * 
	 * @param host
	 */
	public Master(String host) {
		this.host = host;
	}

	public void startZk() throws IOException {
		zk = new ZooKeeper(host, 15000, this);
	}

	public void stopZk() throws IOException, InterruptedException {
		zk.close();
	}

	public void process(WatchedEvent e) {
		masterLog.info("Processing event: " + e.toString());
		if (e.getType() == Event.EventType.None) {
			switch (e.getState()) {
			case SyncConnected:
				connected = true;
				break;
			case Disconnected:
				connected = false;
				break;
			case Expired:
				expired = true;
				connected = false;
				masterLog.error("Session expired");
				break;
			default:
				break;
			}
		}
	}

	/**
	 * Creates the znodes that we will need with an empty byte array because we
	 * do not have any data to put in them.<br>
	 * <br>
	 * 
	 * servers: holds information about the servers connected to the system.<br>
	 * srvUpAvgTime: holds the average time that a server is running. <br>
	 * srvTotalRuntime: holds the total runtime of a server.<br>
	 * srvClients: holds the number of clients that has each server.<br>
	 * connections: keeps all the connections of a server and its clients.<br>
	 * 
	 */
	public void bootstrap() {
		createParent("/servers", new byte[0]);
		createParent("/srvUpAvgTime", new byte[0]);
		createParent("/srvTotalRuntime", new byte[0]);
		createParent("/srvClients", new byte[0]);
		createParent("/connections", new byte[0]);
	}

	void createParent(String path, byte[] data) {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
				createParentCallback, data);
	}

	StringCallback createParentCallback = new StringCallback() {

		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				createParent(path, (byte[]) ctx);
				break;
			case OK:
				masterLog.info("Parent Created");
				break;
			case NODEEXISTS:
				masterLog.info("Parent already registered: " + path);
				break;
			default:
				masterLog.error("Something went wrong: "
						+ KeeperException.create(Code.get(rc), path));
				break;
			}

		}

	};

	/**
	 * Check if this client is connected
	 * 
	 * @return boolean Zookeeper client is connected
	 */
	public boolean isConnected() {
		return connected;
	}

	/**
	 * Check if Zookeeper session is expired
	 * 
	 * @return boolean Zookeeper session has expired
	 */
	public boolean isExpired() {
		return expired;
	}

	/*
	 * ===========================================================
	 * ============*METHODS ABOUT MASTER ELECTION *===============
	 * =============================================================
	 */

	/**
	 * Tries to create a /master lock znode to acquire leadership.
	 */
	public void runForMaster() {
		masterLog.info("Running for master...");
		zk.create("/master", masterId.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL, masterCreateCallback, null);
	}


	StringCallback masterCreateCallback = new StringCallback() {

		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case OK:
				state = MasterStates.ELECTED;
				takeLeadership();
				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				masterExists();
				break;
			default:
				state = MasterStates.NOTELECTED;
				masterLog.error(
						"Something went wrong when running for master...",
						KeeperException.create(Code.get(rc), path));
			}
			masterLog.info("I'm "
					+ (state == MasterStates.ELECTED ? "" : "not ")
					+ "the leader " + masterId);
		}
	};

	void masterExists() {
		zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
	}

	StatCallback masterExistsCallback = new StatCallback() {

		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();
				break;
			case OK:
				break;
			case NONODE:
				state = MasterStates.RUNNING;
				runForMaster();
				masterLog.info("It seems like the previous master is gone,"
						+ "so I am running for master again...");
				break;
			default:
				checkMaster();
				break;
			}
		}
	};

	Watcher masterExistsWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeDeleted) {
				assert "/master".equals(e.getPath());

				runForMaster();
			}
		}
	};
	
	void checkMaster() {
		zk.getData("/master", false, masterCheckCallback, null);
	}
	
	DataCallback masterCheckCallback = new DataCallback() {

		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				runForMaster();
				break;
			case OK:
				if (masterId.equals(new String(data))) {
					state = MasterStates.ELECTED;
					takeLeadership();
				} else {
					state = MasterStates.NOTELECTED;
					masterExists();
				}
				break;

			default:
				masterLog.error("Error when reading data...",
						KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void takeLeadership() {
		masterLog.info("Going for list of servers");
		getServers();
	}
	
	/**
	 * Closes Zookeeper session
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (zk != null) {
			try {
				zk.close();
			} catch (InterruptedException e) {
				masterLog.warn("Interrupted while closing Zookeeper...", e);
			}
		}
	}

	/*
	 * ========================================================================
	 * =========================================================================
	 * 
	 * ==========================================================================
	 * ======== METHODS TO HANDLE CHANGES TO THE LIST OF SERVERS ===============
	 * =========================================================================
	 */

	public int getServersCount() {
		return serversMap.size();
	}

	Watcher serversChangeWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeChildrenChanged) {
				assert "/servers".equals(e.getPath());
				getServers();
			}
		}
	};

	void getServers() {
		zk.getChildren("/servers", serversChangeWatcher,
				serversGetChildrenCallbck, null);
	}

	ChildrenCallback serversGetChildrenCallbck = new ChildrenCallback() {

		public void processResult(int rc, String path, Object ctx,
				List<String> children) {

			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getServers();
				break;
			case OK:
				masterLog.info("Successfully got a list of servers: "
						+ children.size() + " servers");
				updateServerMap(children);

				for (int i = 0; i < children.size(); i++) {
					watchSrvUpAvgTime("/srvUpAvgTime/" + children.get(i), ctx);
					watchSrvTotalRuntime("/srvTotalRuntime/" + children.get(i), ctx);
					watchSrvClients("/srvClients/" + children.get(i), ctx);
				}

				break;
			default:
				masterLog.error("getChildren failed...",
						KeeperException.create(Code.get(rc), path));
			}
		}
	};

	void watchSrvUpAvgTime(String path, Object ctx) {
		zk.exists(path, srvUpAvgTimeWatcher, existsSrvUpAvgTimeCallback, ctx);
	}

	Watcher srvUpAvgTimeWatcher = new Watcher() {

		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged) {
				assert event.getPath().contains("/srvUpAvgTime/server-");

				masterLog.info("[Master]: srvUpAvgTimeWatcher: Node Data Changed..."
								+ event.getPath());
				ServerObj obj;
				obj = serversMap.get(event.getPath().replace("/srvUpAvgTime/",""));
				zk.getData(
						event.getPath(),
						false,
						getSrvUpAvgTimeDataCallback,
						obj);
			}
		}
	};

	StatCallback existsSrvUpAvgTimeCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				watchSrvUpAvgTime(path, ctx);
				break;
			case OK:
				if (stat != null) {
					//zk.getData(path, false, getSrvUpAvgTimeDataCallback, ctx);
					//masterLog.info("[Master]: srvUpAvgTime node is there: " + path);
				}
				break;
			case NONODE:
				break;
			default:
				masterLog.error("[Master]: Something went wrong when "
						+ "checking if the status node exists: "
						+ KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};

	DataCallback getSrvUpAvgTimeDataCallback = new DataCallback() {

		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:		
				ServerObj obj;
				obj = serversMap.get(path.replace("/srvUpAvgTime/", ""));
				zk.getData(path, false, getSrvUpAvgTimeDataCallback, obj);
				return;
			case OK:
				String serverKey = path.replace("/srvUpAvgTime/", "");
				double time = Double.parseDouble(new String(data));
				masterLog.info("[Master]: Server Up Average Time of " + serverKey + ": " + time);

				assert (ctx != null);
				serversMap.get(serverKey).setSrvUpAvgTime(time);

				watchSrvUpAvgTime(path, ctx);
				break;
			case NONODE:
				masterLog.warn("[Master]: " + path + " node is gone!");
				return;
			default:
				masterLog.error("[Master]: Something went wrong here, "
						+ KeeperException.create(Code.get(rc), path));

			}
		}
	};

	void watchSrvTotalRuntime(String path, Object ctx) {
		zk.exists(path, SrvTotalRuntimeWatcher, existsSrvTotalRuntimeCallback, ctx);
	}

	Watcher SrvTotalRuntimeWatcher = new Watcher() {

		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged) {
				assert event.getPath().contains("/srvTotalRuntime/server-");

				masterLog.info("[Master]: SrvTotalRuntimeWatcher: Node Data Changed..."
								+ event.getPath());
				ServerObj obj;
				obj = serversMap.get(event.getPath().replace("/srvTotalRuntime/",""));
				zk.getData(
						event.getPath(),
						false,
						getSrvTotalRuntimeDataCallback,
						obj);
			}
		}
	};

	StatCallback existsSrvTotalRuntimeCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				watchSrvTotalRuntime(path, ctx);
				break;
			case OK:
				if (stat != null) {
					//zk.getData(path, false, getSrvTotalRuntimeDataCallback, ctx);
					//masterLog.info("[Master]: SrvTotalRuntime node is there: " + path);
				}
				break;
			case NONODE:
				break;
			default:
				masterLog.error("[Master]: Something went wrong when "
						+ "checking if the status node exists: "
						+ KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};

	DataCallback getSrvTotalRuntimeDataCallback = new DataCallback() {

		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				ServerObj obj;
				obj = serversMap.get(path.replace("/srvTotalRuntime/", ""));
				zk.getData(path, false, getSrvTotalRuntimeDataCallback, obj);
				return;
			case OK:
				String serverKey = path.replace("/srvTotalRuntime/", "");
				long time = Long.parseLong(new String(data));
				masterLog.info("[Master]: Total Runtime of " + serverKey
						+ ": " + time);

				assert (ctx != null);				
				
				serversMap.get(serverKey).setSrvTotalRuntime(time);
				watchSrvTotalRuntime(path, ctx);
				break;
			case NONODE:
				masterLog.warn("[Master]: " + path + " node is gone!");
				return;
			default:
				masterLog.error("[Master]: Something went wrong here, "
						+ KeeperException.create(Code.get(rc), path));

			}
		}
	};

	void watchSrvClients(String path, Object ctx) {
		zk.exists(path, srvClientsWatcher, existsSrvClientsCallback, ctx);
	}

	Watcher srvClientsWatcher = new Watcher() {

		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged) {
				assert event.getPath().contains("/srvClients/server-");

				masterLog.info("[Master]: srvClientsWatcher: Node Data Changed..."+ event.getPath());

				ServerObj obj;
				obj = serversMap.get(event.getPath().replace("/srvClients/",""));
				
				zk.getData(
						event.getPath(),
						false,
						getSrvClientsDataCallback,
						obj);
			}
		}
	};

	StatCallback existsSrvClientsCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				watchSrvClients(path, ctx);
				break;
			case OK:
				if (stat != null) {
					//zk.getData(path, false, getSrvClientsDataCallback, ctx);
					//masterLog.info("[Master]: srvClients node is there: " + path);
				}
				break;
			case NONODE:
				break;
			default:
				masterLog.error("[Master]: Something went wrong when "
						+ "checking if the status node exists: "
						+ KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};

	DataCallback getSrvClientsDataCallback = new DataCallback() {

		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				ServerObj obj;
				obj = serversMap.get(path.replace("/srvClients/", ""));
				
				zk.getData(path, false, getSrvClientsDataCallback,obj);
				return;
			case OK:
				String serverKey = path.replace("/srvClients/", "");
				int srvClients = Integer.parseInt(new String(data));

				assert (ctx != null);
				serversMap.get(serverKey).setSrvClients(srvClients);
				masterLog.info("[Master]: Number of clients of " + serverKey
							+ ": " + serversMap.get(serverKey).getSrvClients());
					
				watchSrvClients(path, ctx);
				break;
			case NONODE:
				masterLog.warn("[Master]: " + path + " node is gone!");
				return;
			default:
				masterLog.error("[Master]: Something went wrong here, "
						+ KeeperException.create(Code.get(rc), path));

			}
		}
	};

	synchronized void updateServerMap(List<String> children) {
		List<String> addedServers;
		List<String> deletedServers;
		
		addedServers = new ArrayList<String>();
		deletedServers = new ArrayList<String>();

		masterLog.info("[Master]: Updating servers' HashMap...");
		if (serversMap == null || serversMap.size() == 0) {
			serversList = new ChildrenList(children);

			for (String server : serversList.children) {
				getServerIp(server);
			}
		} else {
			ChildrenList temp = new ChildrenList(serversList.children);

			addedServers = serversList.addAndSet(children);
			if (addedServers != null) {
				for (String s : addedServers) {
					getServerIp(s);
				}
			}

			deletedServers = temp.removedAndSet(children);
			System.out.println("SIZE = " + temp.children.size());
			if (deletedServers != null) {
				for (String s : deletedServers)
					serversMap.remove(s);
			}
		}
	}

	/*
	 * =========================================================================
	 * =========================================================================
	 * 
	 * =========================================================================
	 * ======= METHODS FOR RECEIVING NEW CLIENTS AND CONNECTING THEM ===========
	 * =========================================================================
	 */

	/**
	 * Assigns server to client <br>
	 * Gets as parameter the client's Id and returns the server that it has been
	 * assigned in the form of serverId/serverIp
	 * 
	 * @param clientID
	 * @return serverId/serverIp
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public MasterResp selectServer(String clientID) throws IOException,
	// public ServInfo selectServer(String clientID) throws IOException,
			InterruptedException {

		if (serversList.getList().size() == 0) {
			createServer();
		}

		//Selection select = new RandomSelection();
		//Selection select = new CycleServerSelection();
		//Selection select = new LeastServerClientsSelection();
		//Selection select = new LeastServerTotalRuntimeSelection();
		Selection select = new LeastServerUpTimeSelection();
		String designatedServer = select.selectServer(serversMap);

		connectClient(clientID, designatedServer);

		String ipAddress = serversMap.get(designatedServer).getServerIP();
		String ipAddr = ipAddress.substring(ipAddress.indexOf('/') + 1,
				ipAddress.indexOf(':'));
		String port = ipAddress.substring(ipAddress.indexOf(':') + 1,
				ipAddress.length());
		/*
		 * ipAddress = ipAddress.substring(ipAddress.lastIndexOf('/') + 1,
		 * ipAddress.length());
		 */

		masterLog.info("[Master]: -CONNECTION-");
		masterLog.info("[Master]: Client-" + clientID + " connected to Server-"
				+ designatedServer + " noClients = "
				+ serversMap.get(designatedServer).getSrvClients());

		return new MasterResp(new Utf8(designatedServer), new Utf8(ipAddr),
				new Utf8(port));
	}

	/**
	 * Create connection znode between client and selected server
	 * @param client : String
	 * @param designatedServer : String
	 */
	public static void connectClient(String client, String designatedServer) {

		String connectionPath = "/connections/" + designatedServer + "/"
				+ client;
		masterLog.info("[Master]: Connection path: " + connectionPath);
		createConnection(connectionPath, client.getBytes());
	}

	static void createConnection(String path, byte[] data) {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
				connectClientCallback, data);
	}

	static StringCallback connectClientCallback = new StringCallback() {

		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				createConnection(path, (byte[]) ctx);
				break;
			case OK:
				masterLog.info("Client assigned correctly: " + name);
				clToAssignList.remove(name.substring(name.lastIndexOf("/") + 1));
				break;
			case NODEEXISTS:
				masterLog.warn("Client already assigned");
				break;
			default:
				masterLog.error("Error trying to assign client...",
						KeeperException.create(Code.get(rc), path));
			}

		}
	};

	/**
	 * Creates a new server if the servers' list is empty
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void createServer() throws IOException, InterruptedException {

		ExecutorService executorService = Executors.newSingleThreadExecutor();

		executorService.submit(new Runnable() {

			public void run() {
				AvroServer s = new AvroServer(host);
				try {
					s.startZk();
					s.startServer();
					while (!s.isConnected()) {
						Thread.sleep(100);
					}
					s.bootstrap();

					s.register();

					while (!s.isExpired()) {
						Thread.sleep(1000);
					}
				} catch (IOException e) {
					masterLog.warn("[Master]:" + e.getMessage());
				} catch (InterruptedException e) {
					masterLog.warn("[Master]:" + e.getMessage());
				}
			}
		});

		executorService.shutdown();

	}

	/**
	 * Gets the designated server's ip address form /server znode
	 * 
	 * @param serverName : String
	 */
	public static void getServerIp(String serverName) {
		String path = "/servers/" + serverName;
		zk.getData(path, false, getServerDataCallaback, serverName);
	}

	/**
	 * Get server data reassign callback
	 */
	static DataCallback getServerDataCallaback = new DataCallback() {

		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				getServerIp((String) ctx);
				break;
			case OK:
				String ip = "";
				for (byte b : data) {
					ip += (char) b;
					// System.out.print(ip);
				}
				serversMap.put((String) ctx, new ServerObj((String) ctx, ip, 0,
						0));
				masterLog.info("[Master]: --SERVERS-- total {} ",
						serversMap.size());

				Iterator<String> it = serversMap.keySet().iterator();
				while (it.hasNext()) {
					String keySet = (String) it.next();
					masterLog.info(" Key: " + keySet + " IP: "
							+ serversMap.get(keySet).getServerIP()
							+ " Clients: "
							+ serversMap.get(keySet).getSrvUpAvgTime()
							+ " Server Up Time"
							+ serversMap.get(keySet).getSrvUpAvgTime());
				}
				masterLog.info(" ");

				break;
			default:
				masterLog.error(
						"[Master] : Something went wrong when getting data...",
						KeeperException.create(Code.get(rc), path));
			}
		}
	};

	/*
	 * =========================================================================
	 * =========================================================================
	 */

	public static void main(String[] args) throws IOException,
			InterruptedException {
		Master m = new Master(args[0]);
		m.startZk();
		m.startMaster();
		while (!m.isConnected()) {
			Thread.sleep(100);
		}
		m.bootstrap();
		m.runForMaster();
		while (!m.isExpired())
			Thread.sleep(1000);
		m.stopZk();
	}
}
