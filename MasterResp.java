package org.thesis.project.avro.rpc.protocol;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

public class MasterResp extends SpecificRecordBase implements SpecificRecord {

		public static final Schema SCHEMA = new Schema.Parser()
				.parse("{\"type\": \"record\"," + "\"name\": \"MasterResp\","

				+ "\"fields\": ["
						+ " {\"name\": \"serverID\", \"type\": \"string\"},"
						+ "	{\"name\": \"serverIP\", \"type\": \"string\"},"
						+ " {\"name\": \"port\", 	 \"type\": \"string\"}]}");

		public Utf8 serverID;
		public Utf8 serverIP;
		public Utf8 port;

		public MasterResp() {
		}

		public MasterResp(Utf8 serverID, Utf8 serverIP, Utf8 port) {
			this.serverID = serverID;
			this.serverIP = serverIP;
			this.port = port;
		}

		@Override
		public Schema getSchema() {
			return SCHEMA;
		}

		@Override
		public Object get(int field) {
			switch (field) {
			case 0:
				return serverID;
			case 1:
				return serverIP;
			case 2:
				return port;
			default:
				throw new org.apache.avro.AvroRuntimeException("Bad index");
			}
		}

		@Override
		public void put(int field, Object value) {
			switch (field) {
			case 0:
				serverID = (Utf8) value;
			case 1:
				serverIP = (Utf8) value;
				break;
			case 2:
				port = (Utf8) value;
				break;
			default:
				throw new org.apache.avro.AvroRuntimeException("Bad index");
			}
		}
	}
