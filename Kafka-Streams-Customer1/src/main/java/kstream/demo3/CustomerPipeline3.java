package main.java.kstream.demo3;

import main.java.kstream.demo2.ClaimAndPayment;
import main.java.kstream.demo2.CustomerView;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
    private static final String CLAIM_AND_PAYMENT_STORE = "claimAndPayment2Store";

    public static void main(String[] args) {
        StreamsConfig config = new StreamsConfig(KafkaPropertiesUtil.getProperties());

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
