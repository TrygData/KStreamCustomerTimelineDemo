package com.tryg.kafkapoc;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.kafkapoc.config.KafkaPropertiesFactory;
import com.tryg.kafkapoc.config.PocConstants;
import com.tryg.kafkapoc.model.ClaimMessage;
import com.tryg.kafkapoc.model.CustomerMessage;
import com.tryg.kafkapoc.model.PaymentMessage;
import com.tryg.kafkapoc.model.PolicyMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.function.Function;


public class KafkaCustomerFileTopicDispatch {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String BASE_PATH = "inputdata/";

    public static void main(String[] args) throws IOException {
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        KafkaCustomerFileTopicDispatch dispatch = new KafkaCustomerFileTopicDispatch();

        dispatch.sendMessagesFromFile(PocConstants.CUSTOMER_TOPIC, "Customer.txt", CustomerMessage.class, String.class, CustomerMessage::getCustomer);
        dispatch.sendMessagesFromFile(PocConstants.POLICY_TOPIC, "Policy.txt", PolicyMessage.class, Integer.class, PolicyMessage::getPolicy);
        dispatch.sendMessagesFromFile(PocConstants.CLAIM_TOPIC, "Claim.txt", ClaimMessage.class, String.class, ClaimMessage::getClaimNumber);
        dispatch.sendMessagesFromFile(PocConstants.PAYMENT_TOPIC, "Payment.txt", PaymentMessage.class, String.class, PaymentMessage::getClaimNumber);
    }

    private <K, V> void sendMessagesFromFile(String topicName, String fileName, Class<V> payloadType, Class<K> keyClass, Function<V, K> keyFunction) throws IOException {
        Producer producer = new KafkaProducer(KafkaPropertiesFactory.getProducerProperties(keyClass, payloadType));

        ClassLoader classLoader = getClass().getClassLoader();
        String filePath = BASE_PATH + fileName;
        File file = new File(classLoader.getResource(filePath).getFile());

        BufferedReader br = new BufferedReader(new FileReader(file));

        for (String line; (line = br.readLine()) != null; ) {
            System.out.println(line);

            V payload = objectMapper.readValue(line, payloadType);

            ProducerRecord<K, V> record = new ProducerRecord<>(topicName, keyFunction.apply(payload), payload);

            System.out.println("Sending message: " + record);

            producer.send(record);
        }

        producer.close();
    }
}
