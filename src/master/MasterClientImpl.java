package org.thesis.project.master;

import java.io.IOException;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.project.avro.rpc.protocol.Hello;
import org.thesis.project.avro.rpc.protocol.MasterClient;
import org.thesis.project.avro.rpc.protocol.MasterResp;

public class MasterClientImpl implements MasterClient {
	private static final Logger Log = LoggerFactory
			.getLogger(MasterClientImpl.class);
	private final Master master;

	public MasterClientImpl(Master master) {
		this.master = master;
	}


	public MasterResp hello(Hello hello) throws InterruptedException, IOException {
		Log.info("[Master]: Method \"hello\" has been invoked...");
		Log.info("[Master]: clientID = " + hello.clientID);
		Log.info("[Master]: address= " + hello.address);

		Master.clientsList.add(hello.clientID.toString());
		Master.clToAssignList.add(hello.clientID.toString());

		MasterResp server = master.selectServer(hello.clientID.toString());

		for (String c : Master.clientsList)
			System.out.println("Client: " + c);
		

		return server;
	}

	public CharSequence bye(CharSequence resp) throws AvroRemoteException {
		String response = resp.toString();
		String clientId = response.substring(0, response.indexOf('/'));
		String serverId = response.substring(response.indexOf('/')+1 , response.length());
		
		Log.info("[Master]: Method \"bye\" has been invoked...");
		Log.info("[Master]: clientId = " + clientId);

		Master.clientsList.remove(clientId.toString());
		Master.clToAssignList.remove(clientId.toString());
		
		return new Utf8("OK");
	}

}
