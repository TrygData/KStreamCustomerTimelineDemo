package dk.schumacher.setup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class KafkaCustomerFileTopicDispatch {

	public static void main(String[] args) throws IOException, InterruptedException, JSONException {

		Properties producerProperties = KafkaUtilities.getKafkaProducer1Properties();

		System.out.println("Properties: " + producerProperties.toString());

		KafkaProducer<String, String> producerPayment = extractFileToTopic(producerProperties, "/Users/GSConsulting/Tryg/Kafka Example Code/poc/KStreamsDemo-master/Payment.txt", "STATPEJ.POC_CLAIMPAYMENT_DECODED", false);
		KafkaProducer<String, String> producerCustomer = extractFileToTopic(producerProperties, "/Users/GSConsulting/Tryg/Kafka Example Code/poc/KStreamsDemo-master/Customer.txt", "STATPEJ.POC_CUSTOMER_DECODED", true);
		KafkaProducer<String, String> producerClaim = extractFileToTopic(producerProperties, "/Users/GSConsulting/Tryg/Kafka Example Code/poc/KStreamsDemo-master/Claim.txt", "STATPEJ.POC_CLAIM_DECODED", false);
		KafkaProducer<String, String> producerPolicy = extractFileToTopic(producerProperties, "/Users/GSConsulting/Tryg/Kafka Example Code/poc/KStreamsDemo-master/Policy.txt", "STATPEJ.POC_POLICY_DECODED", false);

		Thread.sleep(1000);
		producerPayment.close();
		producerCustomer.close();
		producerClaim.close();
		producerPolicy.close();
	}

	private static KafkaProducer<String, String> extractFileToTopic(Properties producerProperties, String file, String topicName, boolean keyIsString) throws IOException {
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			for (String line; (line = br.readLine()) != null;) {
				System.out.println(line);
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
