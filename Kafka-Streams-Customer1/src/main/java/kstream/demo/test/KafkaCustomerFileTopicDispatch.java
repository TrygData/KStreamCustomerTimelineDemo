package kstream.demo.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaCustomerFileTopicDispatch {

	private KafkaProducer<String, String> kafkaProducer = null;

	public KafkaCustomerFileTopicDispatch(KafkaProducer<String, String> kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
	}

	public void sendFileToTopic(String filePath, String topicName) throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			for (String line; (line = br.readLine()) != null;) {
				System.out.println(line);
				this.kafkaProducer.send(new ProducerRecord<String, String>(topicName, line));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, JSONException {
		Properties producerProperties = KafkaUtilities.getKafkaProducer1Properties();
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

		KafkaCustomerFileTopicDispatch dispatcher = new KafkaCustomerFileTopicDispatch(producer);

		dispatcher.sendFileToTopic("Customer.txt", "STATPEJ.POC_CUSTOMER_DECODED");
		dispatcher.sendFileToTopic("Policy.txt"  , "STATPEJ.POC_POLICY_DECODED");
		dispatcher.sendFileToTopic("Claim.txt"   , "STATPEJ.POC_CLAIM_DECODED");
		dispatcher.sendFileToTopic("Payment.txt" , "STATPEJ.POC_CLAIMPAYMENT_DECODED");

		Thread.sleep(1000);
		producer.close();
	}



}
