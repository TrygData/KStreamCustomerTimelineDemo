package kstream.demo.test;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaUtilities {

	public static Properties getKafkaProducer1Properties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers",
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	public static Properties getKafkaProducer2Properties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "10.84.128.17:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	/**
	 * @param consumerID
	 * @return
	 */
	public static Properties getKafkaConsumerProperties(long consumerID) {
		Properties props = new Properties();
		props.put("bootstrap.servers",
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
		props.put("group.id", "GroupID" + consumerID);
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);

		return props;
	}

	/**
	 * @param consumerID
	 * @return
	 */
	public static Properties getKafkaConsumerPropertiesByte(long consumerID) {
		Properties props = new Properties();
		props.put("bootstrap.servers",
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
		props.put("group.id", "GroupID" + consumerID);
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);

		return props;
	}

}
