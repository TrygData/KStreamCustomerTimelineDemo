package dk.schumacher.poc;

import dk.schumacher.model.Messages;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

import static dk.schumacher.model.Messages.*;

public class POCKafkaStreams {

    private static final String CUSTOMER_TOPIC = "STATPEJ.POC_CUSTOMER_DECODED";
    private static final String POLICY_TOPIC = "STATPEJ.POC_POLICY_DECODED";
    protected static final String CLAIM_TOPIC = "STATPEJ.POC_CLAIM_DECODED";
    protected static final String PAYMENT_TOPIC = "STATPEJ.POC_CLAIMPAYMENT_DECODED";

    private static final String CUSTOMER_STORE = "CustomerStore";
    private static final String POLICY_STORE = "PolicyStore";
    private static final String CLAIM_STORE = "ClaimStrStore";
    private static final String PAYMENT_STORE = "PaymentStore";

    private static final String CUSTOMER_VIEW_OUT = "CA_DEMO_OUTPUT1";

    public static void main(String[] args) {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        Properties props = streamProperties(bootstrapServers, schemaRegistryUrl);
        System.out.println("Properties: " + props);
        StreamsConfig config = new StreamsConfig(props);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> integerSerde = Serdes.Integer();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        /****************************************************************************************************
         * KSTREAMS DEFINITIONS
         ****************************************************************************************************/
        KStream<String, Messages.CustomerMessage> customerStream = kStreamBuilder.stream(stringSerde, customerMessageSerde,
                CUSTOMER_TOPIC);
        KStream<Integer, Messages.PolicyMessage> policyStream = kStreamBuilder.stream(integerSerde, policyMessageSerde,
                POLICY_TOPIC);
        KStream<Integer, Messages.ClaimMessage> claimStream = kStreamBuilder.stream(integerSerde, claimMessageSerde,
                CLAIM_TOPIC);
        KStream<Integer, Messages.PaymentMessage> paymentStream = kStreamBuilder.stream(integerSerde, paymentMessageSerde,
                PAYMENT_TOPIC);


        /****************************************************************************************************
         * CUSTOMER
         ****************************************************************************************************/
        //customerStream.print();

        KTable<Integer, CustomerList> customerGrouped = customerStream              // Grouped by POLICY
                .groupBy((key, value) -> Integer.parseInt(value.POLICY), integerSerde, customerMessageSerde)
                .aggregate(CustomerList::new, (ckey, custMessage, customerList) -> {
                    customerList.customerRecords.add(custMessage);
                    return customerList;
                }, customerListSerde, CUSTOMER_STORE);
        customerGrouped.print();

        /****************************************************************************************************
         * POLICY
         ****************************************************************************************************/

        KTable<Integer, PolicyList> policyGrouped = policyStream                    // Grouped by POLICY
                .groupBy((k, policy) -> policy.POLICY, integerSerde, policyMessageSerde)
                .aggregate(PolicyList::new, (policyKey, policyMsg, policyLst) -> {
                    policyLst.policyRecords.add(policyMsg);
                    return (policyLst);
                }, policyListSerde, POLICY_STORE);

        /****************************************************************************************************
         * CLAIM
         ****************************************************************************************************/

        KTable<Integer, ClaimList> claimStrGrouped = claimStream             // Grouped by CLAIMNUMBER ==> NOW GROUPED BY POLICY
                .groupBy((k, claim) -> claim.getPolicy(), integerSerde, claimMessageSerde)
                .aggregate(ClaimList::new, (claimKey, claimMsg, claimLst) -> {
                    claimLst.claimRecords.add(claimMsg);
                    return (claimLst);
                }, claimListSerde, CLAIM_STORE);

        /****************************************************************************************************
         * PAYMENT
         ****************************************************************************************************/

        KTable<Integer, PaymentList> paymentGrouped = paymentStream             // Grouped by CLAIMNUMBER ==> NOW GROUPED BY POLICY
                .groupBy((k, payment) -> payment.getPolicy(), integerSerde, paymentMessageSerde)
                .aggregate(PaymentList::new, (payKey, payMsg, payLst) -> {

                    payLst.paymentRecords.add(payMsg);
                    return (payLst);
                }, paymentListSerde, PAYMENT_STORE);

        /****************************************************************************************************
         * JOIN
         ****************************************************************************************************/

        KTable<Integer, CustomerAndPolicy> customerAndPolicyGroupedKTable = customerGrouped.join(policyGrouped,
                (customer, policy) -> new CustomerAndPolicy(customer, policy));

        KTable<Integer, ClaimAndPayment> claimAndPaymentKTable = claimStrGrouped.leftJoin(paymentGrouped,
                (claim, payment) -> new ClaimAndPayment(claim, payment));

        /****************************************************************************************************
         * REMAPPING   --  REMOVED !!!!!
         ****************************************************************************************************/


        /****************************************************************************************************
         * FINAL JOIN
         ****************************************************************************************************/
        KTable<Integer, CustomerPolicyClaimPayment> allJoinedAndCoGrouped = customerAndPolicyGroupedKTable.leftJoin(
                claimAndPaymentKTable, (left, right) -> new CustomerPolicyClaimPayment(left, right));

        /****************************************************************************************************
         * KEY TRANSFORMATON
         ****************************************************************************************************/
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
            if (all.claimAndPayment != null) {

                if(all.claimAndPayment.claimList != null) {
                    for (ClaimMessage claim : all.claimAndPayment.claimList.claimRecords) {
                        view.claimRecords.add(claim);
                    }
                }

                if(all.claimAndPayment.paymentList != null) {
                    for (PaymentMessage pay : all.claimAndPayment.paymentList.paymentRecords) {
                        view.paymentRecords.add(pay);
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

        System.out.println("Starting Kafka Streams Customer Demo");

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    private static Properties streamProperties(String bootstrapServers, String schemaRegistryUrl) {

        //Copied from demo
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, String.valueOf(System.currentTimeMillis()));
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/tempStore");
        settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 7500 * 1024 * 1024L);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        // settings.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
//        settings.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
//        // settings.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
//        settings.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
//        // settings.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
//        settings.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
        return settings;
    }

}
