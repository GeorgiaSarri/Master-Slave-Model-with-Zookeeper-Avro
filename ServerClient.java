package org.thesis.project.avro.rpc.protocol;

import java.text.ParseException;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.zookeeper.KeeperException;

public interface ServerClient {
	
	public static final Protocol PROTOCOL = Protocol
			.parse("{\"namespace\": \"org.thesis.project.avro.rpc.protocol\","
					+ " \"protocol\": \"ServerClient\","
					+ " \"doc\": \"An exchange of data between server and client.\","
					+

					" \"types\": ["
					+ " 			  {\"name\": \"Message\", \"type\": \"record\","
					+ " 			   \"fields\": ["
					+ " 			  		{\"name\": \"clientID\",  \"type\": \"string\"},"
					+ "						{\"name\": \"status\", 	  \"type\": \"string\"},"
					+ " 			  		{\"name\": \"timestamp\", \"type\": \"string\"},"
					+ " 			  		{\"name\": \"data\",  	  \"type\": \"string\"}"
					+ " 			  ]}"
					+ "			 ],"
					+

					" \"messages\": {"
					+ " 				\"send\": {"
					+ " 					\"request\": [{\"name\": \"message\", \"type\": \"Message\"}],"
					+ " 					\"response\": \"string\"" 
					+ "					}," 
					+ " 				\"bye\": {"
					+ " 					\"request\": [{\"name\": \"bye\", \"type\": \"string\"}],"
					+ " 					\"response\": \"string\"}" 
					+ " 			 }" 
					+ " 			}");

	CharSequence send(Message message) throws AvroRemoteException, ParseException, KeeperException, InterruptedException;
	CharSequence bye(CharSequence  clientID) throws AvroRemoteException, KeeperException, InterruptedException;
	
}
