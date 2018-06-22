package com.tryg.stream;

import static com.tryg.interfaces.ApplicationConstantsInterface.*;
import static com.tryg.interfaces.ApplicationConstantsInterface.PropertyFileKeys.*;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import com.tryg.model.InputJsonModels.ClaimList;
import com.tryg.model.InputJsonModels.ClaimMessage;
import com.tryg.model.InputJsonModels.CustomerList;
import com.tryg.model.InputJsonModels.CustomerMessage;
import com.tryg.model.InputJsonModels.PaymentList;
import com.tryg.model.InputJsonModels.PaymentMessage;
import com.tryg.model.InputJsonModels.PolicyList;
import com.tryg.model.InputJsonModels.PolicyMessage;
import com.tryg.model.IntermediateJsonModel.ClaimAndPayment;
import com.tryg.model.IntermediateJsonModel.CustomerAndPolicy;
import com.tryg.model.IntermediateJsonModel.CustomerPolicyClaimPayment;
import com.tryg.model.OutputJsonModel.ClaimAndPayment2;
import com.tryg.model.OutputJsonModel.CustomerView;
import com.tryg.services.ClaimAndPayment2Service;
import com.tryg.services.ClaimMessageService;
import com.tryg.services.CustomerMessageService;
import com.tryg.services.CustomerViewService;
import com.tryg.services.PaymentMessageService;
import com.tryg.services.PolicyMessageService;
import com.tryg.util.JsonSerdeGeneratorUtil;
import com.tryg.util.LoggerUtil;
import com.tryg.util.PropertiesFileUtil;

public class KstreamPOCApp {

	public static void main(String[] args) {

		System.out.println("Kafka Streams Customer Demo");
		
		/************************ SERVICES *******************/
		
		CustomerMessageService customerMessageService=new CustomerMessageService();
		PolicyMessageService policyMessageService=new PolicyMessageService();
		ClaimMessageService claimMessageService=new ClaimMessageService();
		PaymentMessageService paymentMessageService=new PaymentMessageService();

		/************************ SERIALIZERS/DESERIALIZERS *******************/
		// Create an instance of StreamsConfig from the Properties instance
		StreamsConfig config = new StreamsConfig(getProperties());
		final Serde<Integer> integerSerde = Serdes.Integer();

		/************************************************************************
		 * NESTED
		 ************************************************************************/

		

		// define customerViewserde
		final Serde<CustomerView> customerViewSerde = JsonSerdeGeneratorUtil.getSerde(CustomerView.class);

		/****************************************************************************************************
		 * KSTREAMS DEFINITIONS
		 ****************************************************************************************************/

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, CustomerMessage> customerStream = streamsBuilder.stream(CUSTOMER_TOPIC,Consumed.with(Serdes.String(), customerMessageService.getCustomerMessageSerde()));
		KStream<Integer, PolicyMessage> policyStream = streamsBuilder.stream(POLICY_TOPIC,Consumed.with(Serdes.Integer(),policyMessageService.getPolicyMessageSerde()));
		KStream<Integer, ClaimMessage> claimStream = streamsBuilder.stream(CLAIM_TOPIC,Consumed.with(Serdes.Integer(),claimMessageService.getClaimMessageSerde()));
		KStream<Integer, PaymentMessage> paymentStream = streamsBuilder.stream(PAYMENT_TOPIC,Consumed.with(Serdes.Integer(),paymentMessageService.getPaymentMessageSerde()));

		/****************************************************************************************************
		 * CUSTOMER
		 ****************************************************************************************************/

		KTable<Integer, CustomerList> customerGrouped = customerMessageService
				.getGroupedConsumerData(customerStream);
		
		final Serde<CustomerList> customerListSerde = JsonSerdeGeneratorUtil.getSerde(CustomerList.class);
		customerGrouped.toStream().to("testCustomer",Produced.with(Serdes.Integer(), customerListSerde));

		/****************************************************************************************************
		 * POLICY
		 ****************************************************************************************************/

		KTable<Integer, PolicyList> policyGrouped = policyMessageService.getGroupedPolicyData(policyStream);
		
		final Serde<PolicyList> policyListSerde = JsonSerdeGeneratorUtil.getSerde(PolicyList.class);
		policyGrouped.toStream().to("testPolicy",Produced.with(Serdes.Integer(), policyListSerde));

		/****************************************************************************************************
		 * CLAIM
		 ****************************************************************************************************/

		KTable<String, ClaimList> claimStrGrouped = claimMessageService.getGroupedClaimData(claimStream);
		
		final Serde<ClaimList> claimListSerde = JsonSerdeGeneratorUtil.getSerde(ClaimList.class);
		claimStrGrouped.toStream().to("testClaim",Produced.with(Serdes.String(), claimListSerde));

		/****************************************************************************************************
		 * PAYMENT
		 ****************************************************************************************************/

		KTable<String, PaymentList> paymentGrouped = paymentMessageService.getGroupedPaymentData(paymentStream);
		
		final Serde<PaymentList> paymentListSerde = JsonSerdeGeneratorUtil.getSerde(PaymentList.class);
		paymentGrouped.toStream().to("testPayment",Produced.with(Serdes.String(), paymentListSerde));

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

		KTable<Integer, ClaimAndPayment2> claimAndPayment2IntGroupedTable = new ClaimAndPayment2Service().getGroupedClaimAndPaymentData(claimAndPaymentKStream);
		/****************************************************************************************************
		 * FINAL JOIN
		 ****************************************************************************************************/
		KTable<Integer, CustomerPolicyClaimPayment> allJoinedAndCoGrouped = customerAndPolicyGroupedKTable.leftJoin(
				claimAndPayment2IntGroupedTable, (left, right) -> new CustomerPolicyClaimPayment(left, right));

		/****************************************************************************************************
		 * KEY TRANSFORMATON
		 ****************************************************************************************************/
		KTable<Integer, CustomerView> customerView = new CustomerViewService().getCustomerViewData(allJoinedAndCoGrouped);

		/****************************************************************************************************
		 * FINAL DATA TO OUTPUT
		 ****************************************************************************************************/
		// customerView.print();
		customerView.toStream().to(CUSTOMER_VIEW_OUT,Produced.with(integerSerde, customerViewSerde));

		System.out.println("Starting Kafka Streams Customer Demo");
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
		kafkaStreams.start();

		System.out.println("Now started Customer Demo");

	}

	private static Properties getProperties() {
		Properties settings = new Properties();
		PropertiesFileUtil propertiesFileUtil = new PropertiesFileUtil(PROPERTY_FILE_PATH);
		settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
		settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,propertiesFileUtil.getPropertyValueByKey(BOOTSTRAP_SERVERS_CONFIG));
		settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		settings.put(StreamsConfig.STATE_DIR_CONFIG, propertiesFileUtil.getPropertyValueByKey(STATE_DIR_CONFIG));
		settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.parseInt(propertiesFileUtil.getPropertyValueByKey(STREAMS_NUM_STREAM_THREADS_CONFIG)));
		settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, Long.parseLong(propertiesFileUtil.getPropertyValueByKey(STREAMS_CACHE_MAX_BYTES_BUFFERING_CONFIG)));
		settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, propertiesFileUtil.getPropertyValueByKey(CONSUMER_AUTO_OFFSET_RESET_CONFIG));
		settings.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, propertiesFileUtil.getPropertyValueByKey(CONSUMER_METRICS_RECORDING_LEVEL_CONFIG));
		settings.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, propertiesFileUtil.getPropertyValueByKey(STREAMS_METRICS_RECORDING_LEVEL_CONFIG));
		settings.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, propertiesFileUtil.getPropertyValueByKey(PRODUCER_METRICS_RECORDING_LEVEL_CONFIG));
		return settings;
	}
}