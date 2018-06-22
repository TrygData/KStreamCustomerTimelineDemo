package com.tryg.serde;

import java.util.Map;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * Class for JsonPOJO
 */
public class JsonPOJOSerializer<T> implements Serializer<T> {
	
	ObjectMapper objectMapper = new ObjectMapper();
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
		
	}

	@Override
	public byte[] serialize(String topic, T data) {
		try {
			return objectMapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			throw new SerializationException("Error serializing data",e);
		}
	}

	@Override
	public void close() {
		
		
	}

}
