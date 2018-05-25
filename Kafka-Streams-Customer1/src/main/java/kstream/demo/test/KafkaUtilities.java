package kstream.demo.test;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaUtilities {

	public static Properties getKafkaProducer1Properties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		/*properties.put("bootstrap.servers",
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");*/
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	public static Properties getKafkaProducer2Properties() {
		Properties properties = new Properties();
		/*properties.put("bootstrap.servers", "10.84.128.17:9092"); */
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	/**
	 * @param consumerID
	 * @return
	 */
	public static Properties getKafkaConsumerProperties(long consumerID) {
		Properties properties = new Properties();
		/*
		props.put("bootstrap.servers",
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
		*/
		properties.put("bootstrap.servers", "localhost:9092");

		properties.put("group.id", "GroupID" + consumerID);
		properties.put("auto.offset.reset", "earliest");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);

		return properties;
	}

	/**
	 * @param consumerID
	 * @return
	 */
	public static Properties getKafkaConsumerPropertiesByte(long consumerID) {
		Properties properties = new Properties();
		/*
		props.put("bootstrap.servers",
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
		*/
		properties.put("bootstrap.servers", "localhost:9092");

		properties.put("group.id", "GroupID" + consumerID);
		properties.put("auto.offset.reset", "earliest");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);

		return properties;
	}

}
