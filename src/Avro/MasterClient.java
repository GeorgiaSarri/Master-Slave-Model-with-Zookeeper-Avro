package org.thesis.project.avro.rpc.protocol;

import java.io.IOException;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;

public interface MasterClient {
	public static final Protocol PROTOCOL = Protocol
			.parse("{\"namespace\": \"org.thesis.project.avro.rpc.protocol\","
					+ " \"protocol\": \"MasterClient\","
					+ " \"doc\": \"An exchange of data between master and client. Client sends ID and address and Master sends server's address\","
					+

					" \"types\": ["
					+ " 			  {\"name\": \"Hello\", \"type\": \"record\","
					+ " 			   \"fields\": ["
					+ " 			  		{\"name\": \"clientID\", \"type\": \"string\"},"
					+ " 			  		{\"name\": \"address\", \"type\": \"string\"}"
					+ " 			  ]},"
					+ "				  {\"name\": \"MasterResp\", \"type\": \"record\","
					+ " 			   \"fields\": ["
					+ " 			  		{\"name\": \"serverID\", \"type\": \"string\"},"
					+ "						{\"name\": \"serverIP\", \"type\": \"string\"},"
					+ " 			  		{\"name\": \"port\", 	 \"type\": \"string\"}"
					+ " 			  ]}"
					+ " 			 ],"
					+

					" \"messages\": {"
					+ " 				\"hello\": {"
					+ " 					\"request\": [{\"name\": \"hello\", \"type\": \"Hello\"}],"
					+ " 					\"response\": \"MasterResp\""
					+ "					}," 
					+ " 				\"bye\": {"
					+ " 					\"request\": [{\"name\": \"bye\", \"type\": \"string\"}],"
					+ " 					\"response\": \"string\"}" 
					+ " 			 }" +
					" 			}");
	
	MasterResp hello(Hello hello) throws AvroRemoteException, InterruptedException, IOException;
	CharSequence bye(CharSequence clientId) throws AvroRemoteException;
}
