/** 
 *  @ JSONSerdeUtils.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
/** 
 * Description Class defines generic method specific to the JSON serialization and Deserialization 
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class SerdeUtils {
	/**
	 * Method convert a POJO class into serialized format
	 * @param input POJO class
	 * @return serialized JSON object
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static <T, customClass> Serializer<customClass> getJSONSerializer(Class<T> customClass)
		    throws IllegalAccessException, InstantiationException {
			Map<String, Object> serdeValues = new HashMap<String, Object>();
			final Serializer<customClass> MessageSerializer = new JsonPOJOSerializer<>();
			serdeValues.put("JsonPOJOClass", customClass);
			MessageSerializer.configure(serdeValues, false);

		    return MessageSerializer;
		}
	/**
	 * Method convert a POJO class into Deserialized format
	 * @param input POJO class
	 * @return Deserialized JSON object
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static <T, customClass> Deserializer<customClass> getJSONDeSerializer(Class<T> customClass)
		    throws IllegalAccessException, InstantiationException {
			Map<String, Object> serdeValues = new HashMap<String, Object>();
			final Deserializer<customClass> MessageDSerializer = new JsonPOJODeserializer<>();
			serdeValues.put("JsonPOJOClass", customClass);
			MessageDSerializer.configure(serdeValues, false);

		    return MessageDSerializer;
		}
	/**
	 * 
	 * @param input serialized custom class
	 * @param output Deserialized custom class
	 * @return  serde 
	 */
	public static <T> Serde<T> gerJsonSerde(Serializer<T> input,Deserializer<T> output)
	{
		 return Serdes.serdeFrom(input,output);
	}
	
	/**
	 * @param class type of serde
	 * 
	 * 
	 */
	  public static <T> Serde<T> getSerdefromClass(Class<T> inputClass) { 
		         
				return Serdes.serdeFrom(inputClass);
				
		      } 

	
}
