package kstream.demo;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomerPipelineDedupFormattedDup {

	static public class CustomerMessage {
		public String ADDRESS;
		public String CUSTOMER;
		public Double CUSTOMERTIME;
		public String POLICY;
	}

	static public class PolicyMessage {
		public int PVAR1;
		public String POLICYENDTIME;
		public int POLICY;
		public String POLICYSTARTTIME;
		public int PVAR0;
	}

	static public class CustomerList {
		public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
		String s = new String();

		public CustomerList() {
		}
	}

	static public class PolicyList {
		public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();

		public PolicyList() {
		}
	}

	static public class ClaimMessage {
		public Double CLAIMTIME;
		public String CLAIMNUMBER;
		public Double CLAIMREPORTTIME;
		public String CLAIMCOUNTER;
	}

	static public class ClaimList {
		public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();

		public ClaimList() {
		}
	}

	static public class PaymentMessage {
		public Double PAYMENT;
		public Double PAYTIME;
		public Integer CLAIMCOUNTER;
		public String CLAIMNUMBER;
	}

	static public class PaymentList {
		public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

		public PaymentList() {
		}
	}

	static public class CustomerAndPolicy {
		public CustomerList customerList = new CustomerList();
		public PolicyList policyList = new PolicyList();

		public CustomerAndPolicy() {
		}

		public CustomerAndPolicy(CustomerList customerList, PolicyList policyList) {
			this.customerList = customerList;
			this.policyList = policyList;
		}

	}

	static public class ClaimAndPayment {
		public ClaimList claimList = new ClaimList();
		public PaymentList paymentList = new PaymentList();

		public ClaimAndPayment() {
		}

		public ClaimAndPayment(ClaimList claimList, PaymentList paymentList) {
			this.claimList = claimList;
			this.paymentList = paymentList;
		}
	}

	static public class ClaimAndPayment2 {
		public Map<String, ClaimAndPayment> claimAndPaymentMap = new HashMap<>();

		public ClaimAndPayment2() {
		}

		public void add(final ClaimAndPayment claimAndPaymentMessage) {
			String key = claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMNUMBER
					+ claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMTIME.toString();
			if (claimAndPaymentMap.containsKey(key)) {
				remove(key);
			}
			claimAndPaymentMap.put(key, claimAndPaymentMessage);
		}

		private void remove(final String key) {
			claimAndPaymentMap.remove(key);
		}
	}

	static public class CustomerPolicyClaimPayment {
		public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
		public ClaimAndPayment2 claimAndPayment2 = new ClaimAndPayment2();

		public CustomerPolicyClaimPayment() {
		}

		public CustomerPolicyClaimPayment(CustomerAndPolicy customerAndPolicy, ClaimAndPayment2 claimAndPayment2) {
			this.customerAndPolicy = customerAndPolicy;
			this.claimAndPayment2 = claimAndPayment2;
		}
	}

	static public class CustomerView {
		public int cutomerKey;
		public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
		public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();
		public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();
		public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

		public CustomerView() {
		}
	}

	private static final String APP_ID = String.valueOf(System.currentTimeMillis());
	private static final String CUSTOMER_TOPIC = "STATPEJ.POC_CUSTOMER_DECODED";
	private static final String POLICY_TOPIC = "STATPEJ.POC_POLICY_DECODED";
	private static final String CLAIM_TOPIC = "STATPEJ.POC_CLAIM_DECODED";
	private static final String PAYMENT_TOPIC = "STATPEJ.POC_CLAIMPAYMENT_DECODED";
	private static final String CUSTOMER_VIEW_OUT = "ZEDBPIH.DEMO_OUTPUT2";
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
		Map<String, Object> serdeProps = new HashMap<String, Object>();
		final Serializer<CustomerMessage> customerMessageSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", CustomerMessage.class);
		customerMessageSerializer.configure(serdeProps, false);

		final Deserializer<CustomerMessage> customerMessageDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", CustomerMessage.class);
		customerMessageDeserializer.configure(serdeProps, false);
		final Serde<CustomerMessage> customerMessageSerde = Serdes.serdeFrom(customerMessageSerializer,
				customerMessageDeserializer);

		// define customerListSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<CustomerList> customerListSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", CustomerList.class);
		customerListSerializer.configure(serdeProps, false);

		final Deserializer<CustomerList> customerListDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", CustomerList.class);
		customerListDeserializer.configure(serdeProps, false);
		final Serde<CustomerList> customerListSerde = Serdes.serdeFrom(customerListSerializer,
				customerListDeserializer);

		// define policySerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<PolicyMessage> policyMessageSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", PolicyMessage.class);
		policyMessageSerializer.configure(serdeProps, false);

		final Deserializer<PolicyMessage> policyMessageDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", PolicyMessage.class);
		policyMessageDeserializer.configure(serdeProps, false);
		final Serde<PolicyMessage> policyMessageSerde = Serdes.serdeFrom(policyMessageSerializer,
				policyMessageDeserializer);

		// define policyListSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<PolicyList> policyListSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", PolicyList.class);
		policyListSerializer.configure(serdeProps, false);

		final Deserializer<PolicyList> policyListDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", PolicyList.class);
		policyListDeserializer.configure(serdeProps, false);
		final Serde<PolicyList> policyListSerde = Serdes.serdeFrom(policyListSerializer, policyListDeserializer);

		// define claimMessageSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<ClaimMessage> claimMessageSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", ClaimMessage.class);
		claimMessageSerializer.configure(serdeProps, false);

		final Deserializer<ClaimMessage> claimMessageDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", ClaimMessage.class);
		claimMessageDeserializer.configure(serdeProps, false);
		final Serde<ClaimMessage> claimMessageSerde = Serdes.serdeFrom(claimMessageSerializer,
				claimMessageDeserializer);

		// define claimListSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<ClaimList> claimListSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", ClaimList.class);
		claimListSerializer.configure(serdeProps, false);

		final Deserializer<ClaimList> claimListDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", ClaimList.class);
		claimListDeserializer.configure(serdeProps, false);
		final Serde<ClaimList> claimListSerde = Serdes.serdeFrom(claimListSerializer, claimListDeserializer);

		// define paymentSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<PaymentMessage> paymentMessageSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", PaymentMessage.class);
		paymentMessageSerializer.configure(serdeProps, false);

		final Deserializer<PaymentMessage> paymentMessageDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", PaymentMessage.class);
		paymentMessageDeserializer.configure(serdeProps, false);
		final Serde<PaymentMessage> paymentMessageSerde = Serdes.serdeFrom(paymentMessageSerializer,
				paymentMessageDeserializer);

		// define paymentListSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<PaymentList> paymentListSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", PaymentList.class);
		paymentListSerializer.configure(serdeProps, false);

		final Deserializer<PaymentList> paymentListDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", PaymentList.class);
		paymentListDeserializer.configure(serdeProps, false);
		final Serde<PaymentList> paymentListSerde = Serdes.serdeFrom(paymentListSerializer, paymentListDeserializer);

		/************************************************************************
		 * NESTED
		 ************************************************************************/

		// define claimAndPaymentSerde
		serdeProps = new HashMap<String, Object>();
		final Serializer<ClaimAndPayment> claimAndPaymentSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
		claimAndPaymentSerializer.configure(serdeProps, false);

		final Deserializer<ClaimAndPayment> claimAndPaymentDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
		claimAndPaymentDeserializer.configure(serdeProps, false);
		final Serde<ClaimAndPayment> claimAndPaymentSerde = Serdes.serdeFrom(claimAndPaymentSerializer,
				claimAndPaymentDeserializer);

		// define claimAndPayment2Serde
		serdeProps = new HashMap<String, Object>();
		final Serializer<ClaimAndPayment2> claimAndPayment2Serializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
		claimAndPayment2Serializer.configure(serdeProps, false);

		final Deserializer<ClaimAndPayment2> claimAndPayment2Deserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
		claimAndPayment2Deserializer.configure(serdeProps, false);
		final Serde<ClaimAndPayment2> claimAndPayment2Serde = Serdes.serdeFrom(claimAndPayment2Serializer,
				claimAndPayment2Deserializer);

		// define customerViewserde
		serdeProps = new HashMap<String, Object>();
		final Serializer<CustomerView> customerViewSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", CustomerView.class);
		customerViewSerializer.configure(serdeProps, false);

		final Deserializer<CustomerView> customerViewDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", CustomerView.class);
		customerViewDeserializer.configure(serdeProps, false);
		final Serde<CustomerView> customerViewSerde = Serdes.serdeFrom(customerViewSerializer,
				customerViewDeserializer);

		/****************************************************************************************************
		 * KSTREAMS DEFINITIONS
		 ****************************************************************************************************/

		KStreamBuilder kStreamBuilder = new KStreamBuilder();

		KStream<String, CustomerMessage> customerStream = kStreamBuilder.stream(stringSerde, customerMessageSerde,
				CUSTOMER_TOPIC);
		KStream<Integer, PolicyMessage> policyStream = kStreamBuilder.stream(integerSerde, policyMessageSerde,
				POLICY_TOPIC);
		KStream<Integer, ClaimMessage> claimStream = kStreamBuilder.stream(integerSerde, claimMessageSerde,
				CLAIM_TOPIC);
		KStream<Integer, PaymentMessage> paymentStream = kStreamBuilder.stream(integerSerde, paymentMessageSerde,
				PAYMENT_TOPIC);

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