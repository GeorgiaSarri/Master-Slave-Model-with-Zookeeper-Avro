package org.thesis.project.avro.rpc.protocol;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

public class Hello extends SpecificRecordBase implements SpecificRecord {
	public static final Schema SCHEMA = new Schema.Parser()
			.parse("{\"type\": \"record\","
					+ "\"name\": \"Hello\","

			+ "\"fields\": ["
					+ "{\"name\": \"clientID\", \"type\": \"string\"},"
					+ "{\"name\": \"address\", \"type\": \"string\"}]}");

	public Utf8 clientID;
	public Utf8 address;
	
	public Hello () { }
	
	public Hello(Utf8 clientID, Utf8 address) {
		this.clientID = clientID;
		this.address = address;
	}
	
	@Override
	public Schema getSchema() {
		return SCHEMA;
	}

	@Override
	public Object get(int field) {
		switch(field){
		case 0:
			return clientID;
		case 1:
			return address;
		 default: throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	@Override
	public void put(int field, Object value) {
		switch(field) {
		case 0: clientID = (Utf8)value; break;
		case 1: address = (Utf8)value; break;
		default: throw new org.apache.avro.AvroRuntimeException("Bad index");
		}

	}

}
