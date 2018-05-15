package kstream.demo.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

public class KafkaCustomerFileTopicDispatch {

	public static void main(String[] args) throws IOException, InterruptedException, JSONException {

		Properties producerProperties = KafkaUtilities.getKafkaProducer1Properties();

		String file = "C:\\Users\\ca-devops\\Desktop\\KStreamsDemo-master\\Payment.txt";

		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			for (String line; (line = br.readLine()) != null;) {
				System.out.println(line);
				producer.send(new ProducerRecord<String, String>("KTABLE_TEST10", line));

			}
		}

		Thread.sleep(1000);
		producer.close();
	}

}
