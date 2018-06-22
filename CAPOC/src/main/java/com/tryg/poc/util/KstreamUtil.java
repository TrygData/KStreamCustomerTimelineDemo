
/** 
 *  @ KstreamUtil.java v1.0   16/05/2018 
 *  Define methods specific to Kstream 
 *  */ 
package com.tryg.poc.util;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
/** 
 * Description
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class KstreamUtil {
	/**
	 * 
	 * @param KstreamBuilder
	 * @return boolean
	 */
	public boolean startKstreams(KStreamBuilder builder)
	{
		boolean isStarted=true;
		try {
			KafkaStreams streams = new KafkaStreams(builder,KafkaUtilities.getConsumerProperites());
			streams.start();
		} catch (StreamsException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			
			e.printStackTrace();
			isStarted=false;
			
		}
		return isStarted;
		
	}
	
	/**
	 * get the KstreamBuilder instance 
	 * @return KsreamBuilder instance
	 */
	public KStreamBuilder getKKStreamBuilder()
	{
		return new KStreamBuilder();
	}
	
	
	/**
	 * @param KstreamBuilder
	 * @param inputTopic
	 * @param JSON POJO class
	 * @param Kafka topic 
	 * @return Kstream with key as string
	 */
	
	 @SuppressWarnings("unchecked")
	public <T, customClass> KStream<String, customClass> getStringKeyKstream(KStreamBuilder builder,String inputTopic,Class<T> customClass)
	 {
		 KStream<String, customClass> kStream=null;
		 try {
			kStream = builder.stream(Constants.stringSerde, SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(customClass),
					  SerdeUtils.getJSONDeSerializer(customClass)), inputTopic);
		} catch (IllegalAccessException | InstantiationException e) {
			e.printStackTrace();
		}
		return kStream;
		 
	 }
	
	/**
	 * @param Kstreambuilder
	 * @param inputtopic
	 * @param custoemClass
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 * 
	 * 
	 */
	@SuppressWarnings("unchecked")
	public <K, V> KStream<K, V> getKstream(KStreamBuilder builder,String inputTopic,Class<V> customClass,Class<K> keyType) throws IllegalAccessException, InstantiationException
	 {
		
		
		return builder.stream(SerdeUtils.getSerdefromClass(keyType), SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(customClass),
				  SerdeUtils.getJSONDeSerializer(customClass)), inputTopic);
		 
	 }
	
}
