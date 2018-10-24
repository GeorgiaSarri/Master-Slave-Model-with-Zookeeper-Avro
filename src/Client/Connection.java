package org.thesis.project.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.Date;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.avro.rpc.protocol.Message;
import org.thesis.project.avro.rpc.protocol.ServerClient;

public class Connection implements Closeable {
	
	private static final Logger Log = LoggerFactory.getLogger(Client.class);
		
	private final String 	ipAddress;
	private final int		port;
	private final String 	clientId;
	
	private NettyTransceiver srvClieTrans;
	private ServerClient proxy;
	
	public Connection(String clientId, String ipAddress, int port) {
		this.clientId 	= clientId;
		this.ipAddress 	= ipAddress;
		this.port 		= port;
	}
	
	public void initSrvClientConnection(String status, String data) {
		try {
			srvClieTrans = new NettyTransceiver(new InetSocketAddress(ipAddress, port));
			proxy = (ServerClient) SpecificRequestor.getClient(ServerClient.class, srvClieTrans);
			
			Message message = new Message(new Utf8(clientId.toString()), 
										  new Utf8(status), 
										  new Utf8(String.valueOf(new Date().getTime())), 
										  new Utf8(data));
						
			String resp = sendMessage(message);
			
			Log.info("[Connection]: Reply from server: "+resp);
			
		} catch (IOException e) {
			Log.error("[Connection]: "+ e.getMessage());
			
		} catch (ClassNotFoundException e) {
			Log.error("[Connection]:"+ e.getMessage());				
			
		} catch (ParseException e) {
			Log.error("[Connection]:"+ e.getMessage());				
			
		} catch (KeeperException e) {
			Log.error("[Connection]:"+ e.getMessage());				
			
		} catch (InterruptedException e) {
			Log.error("[Connection]:"+ e.getMessage());				
			
		}	
	}
	
	public String doAction(String status, String data) {
		String result;
		Log.error("[Connection]: Doing "+status+" action");
		Message message = new Message( new Utf8(clientId.toString()), 
									   new Utf8(status), 
									   new Utf8(String.valueOf(new Date().getTime())),
									   new Utf8(data));
		
		try {
			result = sendMessage(message);
		} catch (ClassNotFoundException e) {
			Log.error("[Connection]:"+ e.getMessage());
			result = "ClassNotFoundException";
		} catch (IOException e) {
			Log.error("[Connection]:"+ e.getMessage());
			result = "IOException";
		} catch (ParseException e) {
			Log.error("[Connection]:"+ e.getMessage());
			result = "ParseException";
		} catch (KeeperException e) {
			Log.error("[Connection]:"+ e.getMessage());
			result = "KeeperException";
		} catch (InterruptedException e) {
			Log.error("[Connection]:"+ e.getMessage());
			result = "InterruptedException";
		}
		
		return result;
	}
	
	public String sendMessage(Message message) throws IOException, ClassNotFoundException, ParseException, KeeperException, InterruptedException {
		Log.info("[Connection] : Method 'sendMessage' has been invoked...");	
		if (srvClieTrans == null)
			return null;
		//proxy = (ServerClient) SpecificRequestor.getClient(ServerClient.class, srvClieTrans);
		return proxy.send(message).toString();
	}
	
	public void close() throws IOException {
		//	Closing connection with server...
		try {
			//ServerClient proxy = (ServerClient) SpecificRequestor.getClient(ServerClient.class, srvClieTrans);
			System.out.println(clientId);
			String resp = proxy.bye(new Utf8(clientId)).toString();
			Log.info("[Connection] Close connection with server :" +resp);
			
		} catch (KeeperException e) {
			Log.error("[Connection]:"+ e.getMessage());
		} catch (InterruptedException e) {
			Log.error("[Connection]:"+ e.getMessage());
		}finally {
			srvClieTrans.close();
		}
	}

}
