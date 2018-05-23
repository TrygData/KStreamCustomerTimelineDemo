package kstream.demo3;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;

public class CustomerPipeline3 {

    private static final String CUSTOMER_TOPIC          = "STATPEJ.POC_CUSTOMER_DECODED";
    private static final String POLICY_TOPIC            = "STATPEJ.POC_POLICY_DECODED";
    private static final String CLAIM_TOPIC             = "STATPEJ.POC_CLAIM_DECODED";
    private static final String PAYMENT_TOPIC           = "STATPEJ.POC_CLAIMPAYMENT_DECODED";
    private static final String CUSTOMER_VIEW_OUT       = "CA_DEMO_OUTPUT1";
    private static final String CUSTOMER_STORE          = "CustomerStore";
    private static final String POLICY_STORE            = "PolicyStore";
    private static final String CLAIM_STORE             = "ClaimStrStore";
    private static final String PAYMENT_STORE           = "PaymentStore";
    private static final String CLAIM_AND_PAYMENT_STORE = "ClaimAndPayment2Store";

    public static void main(String[] args) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> integerSerde = Serdes.Integer();

        final Serde<CustomerMessage>  customerMessageSerde  = createSerde(CustomerMessage.class);
        final Serde<CustomerList>     customerListSerde     = createSerde(CustomerList.class);
        final Serde<PolicyMessage>    policyMessageSerde    = createSerde(PolicyMessage.class);
        final Serde<PolicyList>       policyListSerde       = createSerde(PolicyList.class);
        final Serde<ClaimMessage>     claimMessageSerde     = createSerde(ClaimMessage.class);
        final Serde<ClaimList>        claimListSerde        = createSerde(ClaimList.class);
        final Serde<PaymentMessage>   paymentMessageSerde   = createSerde(PaymentMessage.class);
        final Serde<PaymentList>      paymentListSerde      = createSerde(PaymentList.class);
        final Serde<ClaimAndPayment>  claimAndPaymentSerde  = createSerde(ClaimAndPayment.class);
        final Serde<ClaimAndPayment2> claimAndPayment2Serde = createSerde(ClaimAndPayment2.class);
        final Serde<CustomerView>     customerViewSerde     = createSerde(CustomerView.class);

        // input KStream definitions
        StreamsConfig config = new StreamsConfig(KafkaPropertiesUtil.getProperties());
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<String, CustomerMessage> customerStream = kStreamBuilder.stream(stringSerde , customerMessageSerde, CUSTOMER_TOPIC);
        KStream<Integer, PolicyMessage>  policyStream   = kStreamBuilder.stream(integerSerde, policyMessageSerde  , POLICY_TOPIC);
        KStream<Integer, ClaimMessage>   claimStream    = kStreamBuilder.stream(integerSerde, claimMessageSerde   , CLAIM_TOPIC);
        KStream<Integer, PaymentMessage> paymentStream  = kStreamBuilder.stream(integerSerde, paymentMessageSerde , PAYMENT_TOPIC);

        // join Customer and Policy

        // join Claim and Payment

        // join Customer, Policy, Claim and Payment


        // start stream
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();




    }

    private static <T> Serde<T> createSerde(Class<T> targetClass){
        return Serdes.serdeFrom(createSerializer(targetClass), createDeserializer(targetClass));
    }

    private static <T> Serde<T> createSerde(Serializer<T> serializer, Deserializer<T> deserializer){
        return Serdes.serdeFrom(serializer, deserializer);
    }

    private static <T> Serializer<T> createSerializer(Class<T> forClass){
        final Serializer<T> serializer = new kstream.demo.JsonPOJOSerializer<T>();
        Map<String, Object> serdeProps = new HashMap<String, Object>();
        serdeProps.put("JsonPOJOClass", forClass);
        serializer.configure(serdeProps, false);
        return serializer;
    }

    private static <T> Deserializer<T> createDeserializer(Class<T> forClass) {
        final Deserializer<T> deserializer = new kstream.demo.JsonPOJODeserializer<T>();
        Map<String, Object> serdeProps = new HashMap<String, Object>();
        serdeProps.put("JsonPOJOClass", forClass);
        deserializer.configure(serdeProps, false);
        return deserializer;
    }




}
