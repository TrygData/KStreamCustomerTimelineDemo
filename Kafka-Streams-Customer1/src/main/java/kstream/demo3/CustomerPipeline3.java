package kstream.demo3;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

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

        KGroupedStream<Integer, CustomerMessage> customerMessageKGrouped = customerStream.groupBy((key, customerMessage) -> customerMessage.getPolicyAsInt(), integerSerde, customerMessageSerde);
        KTable<Integer, CustomerList> customerLists = customerMessageKGrouped.aggregate(CustomerList::new, (key, customerMessage, customerList) -> {
            customerList.customerRecords.add(customerMessage);
            return customerList;
        }, customerListSerde, CUSTOMER_STORE);

        /*
        peek(customerLists);
        */

        KGroupedStream<Integer, PolicyMessage> policyMessageKGrouped = policyStream.groupBy((key, policyMessage) -> policyMessage.POLICY, integerSerde, policyMessageSerde);
        KTable<Integer, PolicyList> policyLists = policyMessageKGrouped.aggregate(PolicyList::new, (key, policyMessage, policyList) -> {
            policyList.policyRecords.add(policyMessage);
            return policyList;
        }, policyListSerde, POLICY_STORE);

        /*
        peek(policyLists);
        */


        KGroupedStream<String, ClaimMessage> claimMessageKGroupedStream = claimStream.groupBy((k, claim) -> claim.CLAIMNUMBER, stringSerde, claimMessageSerde);
        KTable<String, ClaimList> claimLists = claimMessageKGroupedStream.aggregate(ClaimList::new, (key, claimMessage, claimList) -> {
            claimList.claimRecords.add(claimMessage);
            return claimList;
        }, claimListSerde, CLAIM_STORE);

        //peek(claimList);

        KGroupedStream<String, PaymentMessage> paymentMessageKGroupedStream = paymentStream.groupBy((k, paymentMessage) -> paymentMessage.CLAIMNUMBER, stringSerde, paymentMessageSerde);
        KTable<String, PaymentList> paymentLists = paymentMessageKGroupedStream.aggregate(PaymentList::new, (key, paymentMessage, paymentList) -> {
            paymentList.paymentRecords.add(paymentMessage);
            return paymentList;
        }, paymentListSerde, PAYMENT_STORE);

        //peek(paymentLists);


        // join Customer and Policy
        KTable<Integer, CustomerAndPolicy> customerAndPolicyGroupedKTable =
                customerLists.join(policyLists, (customer, policy) -> new CustomerAndPolicy(customer, policy));

        //peek(customerAndPolicyGroupedKTable);

        // join Claim and Payment
        KTable<String, ClaimAndPayment> claimAndPaymentKTable = claimLists.leftJoin(paymentLists,
                (claim, payment) -> new ClaimAndPayment(claim, payment));

        //peek(claimAndPaymentKTable);

        KStream<String, ClaimAndPayment> claimAndPaymentKStream = claimAndPaymentKTable.toStream();

        KGroupedStream<Integer, ClaimAndPayment> claimAndPaymentKGroupedStream = claimAndPaymentKStream.groupBy((key, claimPay) -> {
            if (claimPay != null) {
                String claimNumber = claimPay.claimList.claimRecords.get(0).CLAIMNUMBER.split("_")[0];
                return Integer.parseInt(claimNumber);
            } else {
                return 999; // why 999 ?
            }
        }, integerSerde, claimAndPaymentSerde);

        KTable<Integer, ClaimAndPayment2> claimAndPaymentAggregateKTable = claimAndPaymentKGroupedStream.aggregate(ClaimAndPayment2::new, (claimKey, claimPay, claimAndPay2) -> {
            claimAndPay2.add(claimPay);
            return claimAndPay2;
        }, claimAndPayment2Serde, CLAIM_AND_PAYMENT_STORE);


        // join Customer, Policy, Claim and Payment
        KTable<Integer, CustomerPolicyClaimPayment> allJoinedAndCoGrouped = customerAndPolicyGroupedKTable.leftJoin(
                claimAndPaymentAggregateKTable, (left, right) -> new CustomerPolicyClaimPayment(left, right));



        KTable<Integer, CustomerView> customerView = allJoinedAndCoGrouped.<CustomerView>mapValues((all) -> {
            CustomerView view = new CustomerView();
            view.cutomerKey = Integer.parseInt(
                    all.customerAndPolicy.customerList.customerRecords.get(0).CUSTOMER.replaceFirst("cust", ""));
            for (CustomerMessage customer : all.customerAndPolicy.customerList.customerRecords) {
                view.customerRecords.add(customer);
            }
            for (PolicyMessage policy : all.customerAndPolicy.policyList.policyRecords) {
                view.policyRecords.add(policy);
            }
            if (all.claimAndPayment2 != null) {
                for (ClaimAndPayment claimAndPayment : all.claimAndPayment2.claimAndPaymentMap.values()) {

                    for (ClaimMessage claim : claimAndPayment.claimList.claimRecords) {
                        view.claimRecords.add(claim);
                    }
                    if (claimAndPayment.paymentList != null) {
                        for (PaymentMessage payment : claimAndPayment.paymentList.paymentRecords) {
                            view.paymentRecords.add(payment);
                        }
                    }
                }
            }
            return view;
        });

        /****************************************************************************************************
         * FINAL DATA TO OUTPUT
         ****************************************************************************************************/
//		customerView.print();
        customerView.through(integerSerde, customerViewSerde, CUSTOMER_VIEW_OUT);

        // start stream
        System.out.println("Starting Kafka Streams Customer Demo");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.start();

        System.out.println("Now started Customer Demo");
    }

    private static <V,T> void peek(KTable<V, T> kTable) {
        kTable.toStream().peek((key, value) -> {
            System.out.println(key + " : " + value);
        });
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
