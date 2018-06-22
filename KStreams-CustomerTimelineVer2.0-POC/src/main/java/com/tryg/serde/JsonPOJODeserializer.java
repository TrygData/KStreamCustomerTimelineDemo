package com.tryg.serde;

import java.util.Map;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * Class for JsonPOJO
 */

public class JsonPOJODeserializer<T> implements Deserializer<T> {

	private ObjectMapper mapper = new ObjectMapper();
	private Class<T> deserializedClass;
	
	public JsonPOJODeserializer() {
	}

	public JsonPOJODeserializer(Class<T> deserializedClass) {
		this.deserializedClass = deserializedClass;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void configure(Map<String, ?> configs, boolean isKey) {
		if(deserializedClass==null) {
			deserializedClass = (Class<T>) configs.get("JsonPOJOClass");
		}
		
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		// TODO Auto-generated method stub
		if(data == null) {
			return null;
		}
		try {
			return (T)mapper.readValue(data, deserializedClass);
			
		} catch (Exception e) {
			throw new SerializationException("Error deserializing data",e);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
