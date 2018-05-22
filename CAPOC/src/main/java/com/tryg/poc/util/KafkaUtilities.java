
/** 
 *  @ KafkaUtilities.java v1.0   16/05/2018 
 *  Define methods specific to Kafka Connection properties 
 *  */ 
package com.tryg.poc.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
/** 
 * Description
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class KafkaUtilities {

	/***
	 * get the Consumer properties for the kafka connection
	 * @return properties
	 */
	public static Properties getConsumerProperites() {
		
		Properties props = new Properties();
		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KafkaServers+":"+Constants.KafkaBootStrapPort);
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams" + System.currentTimeMillis());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.AUTO_OFFSET_RESET_CONFIG);
		props.setProperty(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.setProperty(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		return props;

	}
	
	/***
	 * get the Consumer properties for the kafka connection
	 * @return properties
	 */
	public static Properties getProducerProperites() {
		
		Properties props = new Properties();
		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KafkaServers+":"+Constants.KafkaBootStrapPort);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return props;

	}

	
}
