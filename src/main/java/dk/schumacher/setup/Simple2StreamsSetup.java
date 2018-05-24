package dk.schumacher.setup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * Copy of example.
 * Create a Topic with 10 partitions manually.
 * Run more than one instance of Stream job and see how groupBy, reduce and aggregate are only performed on the leader node.
 * Start producer (Simple2StreamsSetup) AFTER steam job is started.
 */
public class Simple2StreamsSetup {

	public static void main(String[] args) throws IOException, InterruptedException, JSONException {

		// Create a topic manually with partition:
		// bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic PartitionedTopic1
		// bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic PartitionedTopic2

		Properties producerProperties = KafkaUtilities.getKafkaProducer1Properties();

		System.out.println("Properties: " + producerProperties.toString());

		KafkaProducer<String, String> producerTopic1 = extractFileToTopic(producerProperties, "/Users/GSConsulting/Tryg/KStreamCustomerTimelineDemo/PartitionedTopic1.txt", "PartitionedTopic1", true);
		KafkaProducer<String, String> producerTopic2 = extractFileToTopic(producerProperties, "/Users/GSConsulting/Tryg/KStreamCustomerTimelineDemo/PartitionedTopic2.txt", "PartitionedTopic2", true);

		Thread.sleep(1000);
		producerTopic1.close();
		producerTopic2.close();
	}

	private static KafkaProducer<String, String> extractFileToTopic(Properties producerProperties, String file, String topicName, boolean keyIsString) throws IOException {
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			for (String line; (line = br.readLine()) != null;) {
				System.out.println(line);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				long now = System.currentTimeMillis();
				Random random = new Random();
				int now3 = random.nextInt();
				if(keyIsString)
                    producer.send(new ProducerRecord<String, String>(topicName, String.valueOf(now), line));
				else {
					ProducerRecord<String, String> pr = new ProducerRecord<>(topicName, line);
				    producer.send(pr);
				}

			}
		}
		return producer;
	}

}
