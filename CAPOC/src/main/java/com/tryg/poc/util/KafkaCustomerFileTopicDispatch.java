
/** 
 *  @ KafkaCustomerFileTopicDispatch.java v1.0   16/05/2018 
 *  Define methods create data for customer,payment,policy,claim 
 *  */ 
package com.tryg.poc.util;

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

		Properties producerProperties = KafkaUtilities.getProducerProperites();

		
		String customerFile="Customer.txt";
		String claimFile="Claim.txt";
		String policyFile="Policy.txt";
		String claimPaymentFile="Payment.txt";
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		try (BufferedReader br = new BufferedReader(new FileReader(customerFile))) {
			for (String line; (line = br.readLine()) != null;) {
				System.out.println("customer "+line);
				producer.send(new ProducerRecord<String, String>(Constants.customerTopic, line));

			}
		}
//		try (BufferedReader br = new BufferedReader(new FileReader(claimFile))) {
//			for (String line; (line = br.readLine()) != null;) {
//				System.out.println("claim"+line);
//				producer.send(new ProducerRecord<String, String>(Constants.claimTopic, line));
//
//			}
//		}
//		
//		try (BufferedReader br = new BufferedReader(new FileReader(claimPaymentFile))) {
//			for (String line; (line = br.readLine()) != null;) {
//				System.out.println("claim payment"+line);
//				producer.send(new ProducerRecord<String, String>(Constants.claimPaymentTopic, line));
//
//			}
//		}

//		try (BufferedReader br = new BufferedReader(new FileReader(policyFile))) {
//			for (String line; (line = br.readLine()) != null;) {
//				System.out.println("policy "+line);
//				producer.send(new ProducerRecord<String, String>(Constants.PolicyTopic, line));
//
//			}
//		}

		//produceData(customerFile);
		//produceData(claimFile);
		//produceData(policyFile);
		//produceData(claimPaymentFile);
		Thread.sleep(1000);
		producer.close();
	}

	private static void produceData(String fileName) {
		
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			for (String line; (line = br.readLine()) != null;) {
				System.out.println("88888"+fileName+"====" +line);
				//producer.send(new ProducerRecord<String, String>(Constants.PolicyTopic, line));

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
