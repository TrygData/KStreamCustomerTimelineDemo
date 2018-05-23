package kstream.demo2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomerPipeline2 {

	private static final String APP_ID = String.valueOf(System.currentTimeMillis());
	private static final String CUSTOMER_TOPIC = "STATPEJ.POC_CUSTOMER_DECODED";
	private static final String POLICY_TOPIC = "STATPEJ.POC_POLICY_DECODED";
	private static final String CLAIM_TOPIC = "STATPEJ.POC_CLAIM_DECODED";
	private static final String PAYMENT_TOPIC = "STATPEJ.POC_CLAIMPAYMENT_DECODED";
	private static final String CUSTOMER_VIEW_OUT = "CA_DEMO_OUTPUT1";
	private static final String CUSTOMER_STORE = "CustomerStore";
	private static final String POLICY_STORE = "PolicyStore";
	private static final String CLAIM_STORE = "ClaimStrStore";
	private static final String PAYMENT_STORE = "PaymentStore";
	private static final String CLAIM_AND_PAYMENT_STORE = "claimAndPayment2Store";
	private static final String LEVEL = "DEBUG";

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		System.out.println("Kafka Streams Customer Demo");

		/************************ SERIALIZERS/DESERIALIZERS *******************/
		// Create an instance of StreamsConfig from the Properties instance
		StreamsConfig config = new StreamsConfig(getProperties());
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Integer> integerSerde = Serdes.Integer();

		// define CustomerMessageSerde
		final Serializer<CustomerMessage> customerMessageSerializer = createCustomerMessageSerializer();
		final Deserializer<CustomerMessage> customerMessageDeserializer = createCustomerMessageDeserializer();
		final Serde<CustomerMessage> customerMessageSerde = Serdes.serdeFrom(customerMessageSerializer, customerMessageDeserializer);

		// define customerListSerde
		final Serializer<CustomerList> customerListSerializer = createCustomerListSerializer();
		final Deserializer<CustomerList> customerListDeserializer = createCustomerListDeserializer();
		final Serde<CustomerList> customerListSerde = Serdes.serdeFrom(customerListSerializer, customerListDeserializer);

		// define policySerde
		final Serializer<PolicyMessage> policyMessageSerializer = createPolicyMessageSerializer();
		final Deserializer<PolicyMessage> policyMessageDeserializer = createPolicyMessageDeserializer();
		final Serde<PolicyMessage> policyMessageSerde = Serdes.serdeFrom(policyMessageSerializer, policyMessageDeserializer);

		// define policyListSerde
		final Serializer<PolicyList> policyListSerializer = createPolicyListSerializer();
		final Deserializer<PolicyList> policyListDeserializer = createPolicyListDeserializer();
		final Serde<PolicyList> policyListSerde = Serdes.serdeFrom(policyListSerializer, policyListDeserializer);

		// define claimMessageSerde
		final Serializer<ClaimMessage> claimMessageSerializer = createClaimMessageSerializer();
		final Deserializer<ClaimMessage> claimMessageDeserializer = createClaimMessageDeserializer();
		final Serde<ClaimMessage> claimMessageSerde = Serdes.serdeFrom(claimMessageSerializer, claimMessageDeserializer);

		// define claimListSerde
		final Serializer<ClaimList> claimListSerializer = createClaimListSerializer();
		final Deserializer<ClaimList> claimListDeserializer = createClaimListDeserializer();
		final Serde<ClaimList> claimListSerde = Serdes.serdeFrom(claimListSerializer, claimListDeserializer);

		// define paymentSerde
		final Serializer<PaymentMessage> paymentMessageSerializer = createPaymentMessageSerializer();
		final Deserializer<PaymentMessage> paymentMessageDeserializer = createPaymentMessageDeserializer();
		final Serde<PaymentMessage> paymentMessageSerde = Serdes.serdeFrom(paymentMessageSerializer, paymentMessageDeserializer);


		// define paymentListSerde
		final Serializer<PaymentList> paymentListSerializer = createPaymentListSerializer();
		final Deserializer<PaymentList> paymentListDeserializer = new kstream.demo.JsonPOJODeserializer<PaymentList>();
		final Serde<PaymentList> paymentListSerde = Serdes.serdeFrom(paymentListSerializer, paymentListDeserializer);
		/************************************************************************
		 * NESTED
		 ************************************************************************/

		// define claimAndPaymentSerde
		final Serializer<ClaimAndPayment> claimAndPaymentSerializer = createClaimAndPaymentSerializer();
		final Deserializer<ClaimAndPayment> claimAndPaymentDeserializer = createClaimAndPaymentDeserializer();
		final Serde<ClaimAndPayment> claimAndPaymentSerde = Serdes.serdeFrom(claimAndPaymentSerializer,	claimAndPaymentDeserializer);

		// define claimAndPayment2Serde
		final Serializer<ClaimAndPayment2> claimAndPayment2Serializer = new kstream.demo.JsonPOJOSerializer<ClaimAndPayment2>();
		final Deserializer<ClaimAndPayment2> claimAndPayment2Deserializer = new kstream.demo.JsonPOJODeserializer<ClaimAndPayment2>();
		final Serde<ClaimAndPayment2> claimAndPayment2Serde = Serdes.serdeFrom(claimAndPayment2Serializer, claimAndPayment2Deserializer);

		// define customerViewserde
		final Serializer<CustomerView> customerViewSerializer = new kstream.demo.JsonPOJOSerializer<CustomerView>();
		final Deserializer<CustomerView> customerViewDeserializer = new kstream.demo.JsonPOJODeserializer<CustomerView>();
		final Serde<CustomerView> customerViewSerde = Serdes.serdeFrom(customerViewSerializer, customerViewDeserializer);


		/****************************************************************************************************
		 * KSTREAMS DEFINITIONS
		 ****************************************************************************************************/

		KStreamBuilder kStreamBuilder = new KStreamBuilder();

		KStream<String, CustomerMessage> customerStream = kStreamBuilder.stream(stringSerde , customerMessageSerde, CUSTOMER_TOPIC);
		KStream<Integer, PolicyMessage>  policyStream   = kStreamBuilder.stream(integerSerde, policyMessageSerde  , POLICY_TOPIC);
		KStream<Integer, ClaimMessage>   claimStream    = kStreamBuilder.stream(integerSerde, claimMessageSerde   , CLAIM_TOPIC);
		KStream<Integer, PaymentMessage> paymentStream  = kStreamBuilder.stream(integerSerde, paymentMessageSerde , PAYMENT_TOPIC);


		/****************************************************************************************************
		 * CUSTOMER
		 ****************************************************************************************************/
		KTable<Integer, CustomerList> customerGrouped = customerStream
				.groupBy((key, value) -> Integer.parseInt(value.POLICY), integerSerde, customerMessageSerde)
				.aggregate(CustomerList::new, (ckey, custMessage, customerList) -> {
					customerList.customerRecords.add(custMessage);
					return customerList;
				}, customerListSerde, CUSTOMER_STORE);

		/****************************************************************************************************
		 * POLICY
		 ****************************************************************************************************/

		KTable<Integer, PolicyList> policyGrouped = policyStream
				.groupBy((k, policy) -> policy.POLICY, integerSerde, policyMessageSerde)
				.aggregate(PolicyList::new, (policyKey, policyMsg, policyLst) -> {
					policyLst.policyRecords.add(policyMsg);
					return (policyLst);
				}, policyListSerde, POLICY_STORE);

		/****************************************************************************************************
		 * CLAIM
		 ****************************************************************************************************/

		KTable<String, ClaimList> claimStrGrouped = claimStream
				.groupBy((k, claim) -> claim.CLAIMNUMBER, stringSerde, claimMessageSerde)
				.aggregate(ClaimList::new, (claimKey, claimMsg, claimLst) -> {
					claimLst.claimRecords.add(claimMsg);
					return (claimLst);
				}, claimListSerde, CLAIM_STORE);

		/****************************************************************************************************
		 * PAYMENT
		 ****************************************************************************************************/

		KTable<String, PaymentList> paymentGrouped = paymentStream
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
		kafkaStreams.start();

		System.out.println("Now started Customer Demo");

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


	private static Serializer<CustomerMessage> createCustomerMessageSerializer() {
		final Serializer<CustomerMessage> serializer = new kstream.demo.JsonPOJOSerializer<CustomerMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", CustomerMessage.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<CustomerMessage> createCustomerMessageDeserializer() {
		final Deserializer<CustomerMessage> deserializer = new kstream.demo.JsonPOJODeserializer<CustomerMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", CustomerMessage.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<CustomerList> createCustomerListSerializer() {
		final Serializer<CustomerList> serializer = new kstream.demo.JsonPOJOSerializer<CustomerList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", CustomerList.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<CustomerList> createCustomerListDeserializer() {
		final Deserializer<CustomerList> deserializer = new kstream.demo.JsonPOJODeserializer<CustomerList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", CustomerList.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<PolicyMessage> createPolicyMessageSerializer() {
		final Serializer<PolicyMessage> serializer = new kstream.demo.JsonPOJOSerializer<PolicyMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PolicyMessage.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<PolicyMessage> createPolicyMessageDeserializer() {
		final Deserializer<PolicyMessage> deserializer = new kstream.demo.JsonPOJODeserializer<PolicyMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PolicyMessage.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<PolicyList> createPolicyListSerializer() {
		final Serializer<PolicyList> serializer = new kstream.demo.JsonPOJOSerializer<PolicyList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PolicyList.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<PolicyList> createPolicyListDeserializer() {
		final Deserializer<PolicyList> deserializer = new kstream.demo.JsonPOJODeserializer<PolicyList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PolicyList.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<ClaimMessage> createClaimMessageSerializer() {
		final Serializer<ClaimMessage> serializer = new kstream.demo.JsonPOJOSerializer<ClaimMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimMessage.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<ClaimMessage> createClaimMessageDeserializer() {
		final Deserializer<ClaimMessage> deserializer = new kstream.demo.JsonPOJODeserializer<ClaimMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimMessage.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<ClaimList> createClaimListSerializer() {
		final Serializer<ClaimList> serializer = new kstream.demo.JsonPOJOSerializer<ClaimList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimList.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<ClaimList> createClaimListDeserializer() {
		final Deserializer<ClaimList> deserializer = new kstream.demo.JsonPOJODeserializer<ClaimList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimList.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<PaymentMessage> createPaymentMessageSerializer() {
		final Serializer<PaymentMessage> serializer = new kstream.demo.JsonPOJOSerializer<PaymentMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PaymentMessage.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<PaymentMessage> createPaymentMessageDeserializer() {
		final Deserializer<PaymentMessage> deserializer = new kstream.demo.JsonPOJODeserializer<PaymentMessage>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PaymentMessage.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<PaymentList> createPaymentListSerializer() {
		final Serializer<PaymentList> serializer = new kstream.demo.JsonPOJOSerializer<PaymentList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PaymentList.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<PaymentList> createPaymentListDeserializer() {
		final Deserializer<PaymentList> deserializer = new kstream.demo.JsonPOJODeserializer<PaymentList>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", PaymentList.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<ClaimAndPayment> createClaimAndPaymentSerializer() {
		final Serializer<ClaimAndPayment> serializer = new kstream.demo.JsonPOJOSerializer<ClaimAndPayment>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<ClaimAndPayment> createClaimAndPaymentDeserializer() {
		final Deserializer<ClaimAndPayment> deserializer = new kstream.demo.JsonPOJODeserializer<ClaimAndPayment>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<ClaimAndPayment2> createClaimAndPayment2Serializer() {
		final Serializer<ClaimAndPayment2> serializer = new kstream.demo.JsonPOJOSerializer<ClaimAndPayment2>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<ClaimAndPayment2> createClaimAndPayment2Deserializer() {
		final Deserializer<ClaimAndPayment2> deserializer = new kstream.demo.JsonPOJODeserializer<ClaimAndPayment2>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}

	private static Serializer<CustomerView> createCustomerViewSerializer() {
		final Serializer<CustomerView> serializer = new kstream.demo.JsonPOJOSerializer<CustomerView>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", CustomerView.class);
		serializer.configure(serdeProps, false);
		return serializer;
	}

	private static Deserializer<CustomerView> createCustomerViewDeserializer() {
		final Deserializer<CustomerView> deserializer = new kstream.demo.JsonPOJODeserializer<CustomerView>();
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		serdeProps.put("JsonPOJOClass", CustomerView.class);
		deserializer.configure(serdeProps, false);
		return deserializer;
	}




	private static Properties getProperties() {
		Properties settings = new Properties();
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
		settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/tempStore");
		settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
		settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 7500 * 1024 * 1024L);
		settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// settings.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
		settings.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
		// settings.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
		settings.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
		// settings.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
		settings.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
		return settings;
	}
}