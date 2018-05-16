package dk.schumacher.poc;

import dk.schumacher.avro.Constants;
import dk.schumacher.avro.Messages;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static dk.schumacher.avro.Constants.CUSTOMER_SCHEMA2;
import static dk.schumacher.avro.Messages.*;


public class POCKafkaStreams {

    private static final String APP_ID = "KStreamCustomerTimelineDemo1000";
    private static final String CUSTOMER_TOPIC = "STATPEJ.POC_CUSTOMER_DECODED";
    private static final String POLICY_TOPIC = "STATPEJ.POC_POLICY_DECODED";
    protected static final String CLAIM_TOPIC = "STATPEJ.POC_CLAIM_DECODED";
    protected static final String PAYMENT_TOPIC = "STATPEJ.POC_CLAIMPAYMENT_DECODED";

    private static final String CUSTOMER_STORE = "CustomerStore";
    private static final String POLICY_STORE = "PolicyStore";
    private static final String CLAIM_STORE = "ClaimStrStore";
    private static final String PAYMENT_STORE = "PaymentStore";
    private static final String CLAIM_AND_PAYMENT_STORE = "claimAndPayment2Store";

    private static final String CUSTOMER_VIEW_OUT = "CA_DEMO_OUTPUT1";

    public static void main(String[] args) throws IOException, InterruptedException {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

//        final Schema customerSchema = org.apache.avro.Schema.createRecord("customer", CUSTOMER_SCHEMA2, "", false);
//        final Map<String, Schema> schemas = new HashMap<String, Schema>();
//        schemas.put("", customerSchema);
//        final Schema.Parser parser = new Schema.Parser();
//        parser.addTypes(schemas);
//        System.out.println(Schema.Parser.);
        final Schema CUSTOMER_SCHEMA2 = Schema.parse(Constants.CUSTOMER_SCHEMA2);
        final Serde<GenericRecord> valueGenericCustomerSerde = new GenericAvroSerde();

        Properties props = streamProperties(bootstrapServers, schemaRegistryUrl);
        System.out.println("Properties: " + props);
        StreamsConfig config = new StreamsConfig(props);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

//        KStream<String, String> policyKStream = kStreamBuilder.stream(stringSerde, stringSerde, POLICY_TOPIC);

//        KTable<String, Messages.CustomerMessage> customerKtable = kStreamBuilder.table(stringSerde, customerMessageSerde, CUSTOMER_TOPIC);
//        customerKtable.print();
        //customerKtable.groupBy()

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

        KTable<String, ClaimList> claimStrGrouped = claimStream             // Grouped by CLAIMNUMBER
                .groupBy((k, claim) -> claim.CLAIMNUMBER, stringSerde, claimMessageSerde)
                .aggregate(ClaimList::new, (claimKey, claimMsg, claimLst) -> {
                    claimLst.claimRecords.add(claimMsg);
                    return (claimLst);
                }, claimListSerde, CLAIM_STORE);

        /****************************************************************************************************
         * PAYMENT
         ****************************************************************************************************/

        KTable<String, PaymentList> paymentGrouped = paymentStream             // Grouped by CLAIMNUMBER
                .groupBy((k, payment) -> payment.CLAIMNUMBER, stringSerde, paymentMessageSerde)
                .aggregate(PaymentList::new, (payKey, payMsg, payLst) -> {

                    payLst.paymentRecords.add(payMsg);
                    return (payLst);
                }, paymentListSerde, PAYMENT_STORE);

        /****************************************************************************************************
         * JOIN
         ****************************************************************************************************/

        KTable<Integer, CustomerAndPolicy> customerAndPolicyGroupedKTable = customerGrouped.join(policyGrouped,
                (customer, policy) -> new CustomerAndPolicy(customer, policy));

        KTable<String, ClaimAndPayment> claimAndPaymentKTable = claimStrGrouped.leftJoin(paymentGrouped,
                (claim, payment) -> new ClaimAndPayment(claim, payment));

        /****************************************************************************************************
         * REMAPPING
         ****************************************************************************************************/

        KStream<String, ClaimAndPayment> claimAndPaymentKStream = claimAndPaymentKTable.toStream();

        KTable<Integer, ClaimAndPayment2> claimAndPayment2IntGroupedTable = claimAndPaymentKStream
                .groupBy((k, claimPay) -> (claimPay != null)
                        ? Integer.parseInt(claimPay.claimList.claimRecords.get(0).CLAIMNUMBER.split("_")[0])
                        : 999, integerSerde, claimAndPaymentSerde)
                .aggregate(ClaimAndPayment2::new, (claimKey, claimPay, claimAndPay2) -> {
                    claimAndPay2.add(claimPay);
                    return claimAndPay2;
                }, claimAndPayment2Serde, CLAIM_AND_PAYMENT_STORE);
        /****************************************************************************************************
         * FINAL JOIN
         ****************************************************************************************************/
        KTable<Integer, CustomerPolicyClaimPayment> allJoinedAndCoGrouped = customerAndPolicyGroupedKTable.leftJoin(
                claimAndPayment2IntGroupedTable, (left, right) -> new CustomerPolicyClaimPayment(left, right));

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
