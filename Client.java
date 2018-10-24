package org.thesis.project.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.avro.rpc.protocol.Hello;
import org.thesis.project.avro.rpc.protocol.MasterClient;
import org.thesis.project.avro.rpc.protocol.MasterResp;
import org.thesis.project.avro.rpc.protocol.Status;
import org.thesis.project.client.exceptions.ClientServerException;
import org.thesis.project.client.exceptions.ExceptionCodes;
import org.thesis.project.client.exceptions.UserExpiredException;

public class Client {

	private static final Logger Log = LoggerFactory.getLogger(Client.class);

	private Connection connectSrv;

	private NettyTransceiver masterClientTrans;
	
	private static InetSocketAddress masterAddress = new InetSocketAddress(
			"localhost", 8081);

	private String ipAddr;
	private Integer port;
	private String serverId;
	
	private final UUID clientID;
	private String data = "Hello world!";

	private static class ClientHolder {
		static final Client client = new Client();
	}

	public static Client getInstance() {
		return ClientHolder.client;
	}

	private Client() {
		// Init client and connect with master...
		clientID = UUID.randomUUID();

		try {
			masterClientTrans = new NettyTransceiver(masterAddress);

			Hello hello = new Hello(new Utf8(clientID.toString()), new Utf8(
					masterClientTrans.toString())); // !!!! PENDING HOW TO SAVE
													// MY OWN ADDRESS
			MasterResp response = sayHello(hello);
			ipAddr = response.serverIP.toString();
			port = Integer.parseInt(response.port.toString());
			serverId = response.serverID.toString();
			
			masterClientTrans.close();

		} catch (Exception e) {

			Log.warn("[Client]: Error when initiating client...", e);
		}
	}

	public void newConnection() throws ClientServerException {
		// Connect client with server returned from master...
		if(ipAddr == null || port == null)
			throw new ClientServerException("Invalid server's ip address"+ ipAddr +" or port" + port);
		else {
			connectSrv = new Connection(clientID.toString(), ipAddr, port);
			connectSrv.initSrvClientConnection(Status.INIT.toString(), data);
		}
	}

	public MasterResp sayHello(Hello hello) throws IOException,
			InterruptedException {
		Log.info("[Client] : Method 'sayHello' was invoked...");

		if (masterClientTrans == null)
			return null;
		MasterClient proxy = (MasterClient) SpecificRequestor.getClient(MasterClient.class, masterClientTrans);
		
		return proxy.hello(hello);
	}

	public String doSelect() throws UserExpiredException {
		String result;
		result = connectSrv.doAction((Status.SELECT).toString(), data);
		if(result == null)
			throw new UserExpiredException("Client-"+clientID+": Server down...");
		else if (result.equals(ExceptionCodes.ClientExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Connection expired...");
		else if (result.equals(ExceptionCodes.RequestExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Select expired...");
		else if (result.equals(ExceptionCodes.StatusNotSpecified.toString()))
			throw new UserExpiredException("Client-"+clientID+": Status not specified...");
		return result;
	}
	
	public String doInsert() throws UserExpiredException{
		String result;
		result = connectSrv.doAction((Status.INSERT).toString(), data);
		if(result == null)
			throw new UserExpiredException("Client-"+clientID+": Server down...");
		else if (result.equals(ExceptionCodes.ClientExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Connection expired...");
		else if (result.equals(ExceptionCodes.RequestExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Insert expired...");
		else if (result.equals(ExceptionCodes.StatusNotSpecified.toString()))
			throw new UserExpiredException("Client-"+clientID+": Status not specified...");
		return result;
	}
	
	public String doUpdate() throws UserExpiredException{
		String result;
		result = connectSrv.doAction((Status.UPDATE).toString(), data);
		if(result == null)
			throw new UserExpiredException("Client-"+clientID+": Server down...");
		else if (result.equals(ExceptionCodes.ClientExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Connection expired...");
		else if (result.equals(ExceptionCodes.RequestExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Update expired...");
		else if (result.equals(ExceptionCodes.StatusNotSpecified.toString()))
			throw new UserExpiredException("Client-"+clientID+": Status not specified...");
		return result;
	}
	
	public String doDelete() throws UserExpiredException{
		String result;
		result = connectSrv.doAction((Status.DELETE).toString(), data);
		if(result == null)
			throw new UserExpiredException("Client-"+clientID+": Server down...");
		else if (result.equals(ExceptionCodes.ClientExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Connection expired...");
		else if (result.equals(ExceptionCodes.RequestExpired.toString()))
			throw new UserExpiredException("Client-"+clientID+": Delete expired...");
		else if (result.equals(ExceptionCodes.StatusNotSpecified.toString()))
			throw new UserExpiredException("Client-"+clientID+": Status not specified...");
		return result;
	}
	
	public void stopClient() throws IOException, KeeperException,
			InterruptedException {

		Log.info("[Client] : Method 'close' was invoked...");

		connectSrv.close();

		// Closing connection with master...
		if (masterClientTrans != null)			
				masterClientTrans.close();
		masterClientTrans = new NettyTransceiver(masterAddress);
		MasterClient proxy = (MasterClient) SpecificRequestor.getClient(MasterClient.class, masterClientTrans);

		String resp = proxy.bye(new Utf8(clientID.toString()+"/"+serverId)).toString();
		Log.info("[Client]: Close connection with Master" + resp);
		masterClientTrans.close();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {

		Client c = Client.getInstance();

		// Do stuff
		Log.info("[Client]: Client starts sleeping...");
		Thread.sleep(20000);
		Log.info("[Client]: Client stops sleeping...");

		c.stopClient();
	}
}
