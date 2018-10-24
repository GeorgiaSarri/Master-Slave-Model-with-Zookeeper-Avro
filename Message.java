package org.thesis.project.avro.rpc.protocol;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

public class Message extends SpecificRecordBase implements SpecificRecord {

	public static final Schema SCHEMA = new Schema.Parser()
			.parse("{\"type\": \"record\"," + "\"name\": \"Message\","

			+ "\"fields\": ["
					+ " {\"name\": \"clientID\", \"type\": \"string\"},"
					+ "	{\"name\": \"status\", \"type\": \"string\"},"
					+ "	{\"name\": \"timestamp\", \"type\": \"string\"},"
					+ " {\"name\": \"data\", \"type\": \"string\"}]}");

	public Utf8 clientID;
	public Utf8 status;
	public Utf8 timestamp;
	public Utf8 data;

	public Message() {
	}

	public Message(Utf8 clientID, Utf8 status, Utf8 timestamp, Utf8 data) {
		this.clientID = clientID;
		this.status = status;
		this.timestamp = timestamp;
		this.data = data;
	}

	@Override
	public Schema getSchema() {
		return SCHEMA;
	}

	@Override
	public Object get(int field) {
		switch (field) {
		case 0:
			return clientID;
		case 1:
			return status;
		case 2:
			return timestamp;
		case 3:
			return data;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	@Override
	public void put(int field, Object value) {
		switch (field) {
		case 0:
			clientID = (Utf8) value;
		case 1:
			status = (Utf8) value;
			break;
		case 2:
			timestamp = (Utf8) value;
			break;
		case 3:
			data = (Utf8) value;
			break;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}
}
