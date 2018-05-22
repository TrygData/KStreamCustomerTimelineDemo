package com.tryg.kafkapoc.processor;

import com.tryg.kafkapoc.config.KafkaPropertiesFactory;
import com.tryg.kafkapoc.config.PocConstants;
import com.tryg.kafkapoc.config.StreamsBuilderUtil;
import com.tryg.kafkapoc.model.*;
import com.tryg.kafkapoc.serde.SerdeFactory;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;

@Component
public class PocProcessor {

    private final KafkaPropertiesFactory kafkaPropertiesFactory;
    private final SerdeFactory serdeFactory;

    @Autowired
    public PocProcessor(KafkaPropertiesFactory kafkaPropertiesFactory, SerdeFactory serdeFactory) {
        this.kafkaPropertiesFactory = kafkaPropertiesFactory;
        this.serdeFactory = serdeFactory;
    }

    @PostConstruct
    public void process() {
        StreamsBuilderUtil streamsBuilderUtil = new StreamsBuilderUtil(kafkaPropertiesFactory, serdeFactory);

        KStream<String, CustomerMessage> customers = streamsBuilderUtil.readTopic(PocConstants.CUSTOMER_TOPIC, String.class, CustomerMessage.class);
        KStream<Integer, PolicyMessage> policies = streamsBuilderUtil.readTopic(PocConstants.POLICY_TOPIC, Integer.class, PolicyMessage.class);
        KStream<String, ClaimMessage> claims = streamsBuilderUtil.readTopic(PocConstants.CLAIM_TOPIC, String.class, ClaimMessage.class);
        KStream<String, PaymentMessage> payments = streamsBuilderUtil.readTopic(PocConstants.PAYMENT_TOPIC, String.class, PaymentMessage.class);

        // Group all tables by policy number
        KTable<String, Collection<CustomerMessage>> groupedCustomers = streamsBuilderUtil.groupAndAggregateItems(customers, String.class, CustomerMessage.class, CustomerMessage::getPolicy);
        KTable<String, Collection<PolicyMessage>> groupedPolicies = streamsBuilderUtil.groupAndAggregateItems(policies, String.class, PolicyMessage.class, policyMessage -> String.valueOf(policyMessage.getPolicy()));
        KTable<String, Collection<ClaimMessage>> groupedClaims = streamsBuilderUtil.groupAndAggregateItems(claims, String.class, ClaimMessage.class, claimMessage -> getPolicyNumber(claimMessage.getClaimNumber()));
        KTable<String, Collection<PaymentMessage>> groupedPayments = streamsBuilderUtil.groupAndAggregateItems(payments, String.class, PaymentMessage.class, paymentMessage -> getPolicyNumber(paymentMessage.getClaimNumber()));

        groupedCustomers.toStream().to("grouped-customers", Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getCollectionSerde(CustomerMessage.class)));
        groupedPolicies.toStream().to("grouped-policies", Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getCollectionSerde(PolicyMessage.class)));
        groupedClaims.toStream().to("grouped-claims", Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getCollectionSerde(ClaimMessage.class)));
        groupedPayments.toStream().to("grouped-payments", Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getCollectionSerde(PaymentMessage.class)));

        //Join customers with policies and claims with payments
        KTable<String, GenericCollections<CustomerMessage, PolicyMessage>> customerPolicies = groupedCustomers.leftJoin(groupedPolicies, GenericCollections::new);
        KTable<String, GenericCollections<ClaimMessage, PaymentMessage>> claimPayments = groupedClaims.leftJoin(groupedPayments, GenericCollections::new);

        customerPolicies.toStream().to("customers-policies",
                Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getGenericListsViewSerde(CustomerMessage.class, PolicyMessage.class)));
        claimPayments.toStream().to("claims-payments",
                Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getGenericListsViewSerde(ClaimMessage.class, PaymentMessage.class)));

        /*
         * 1. Join customer-policy and claim-payment streams on policy number into CustomerView
         * 2. Map to stream and change key to customer
         * 3. Print processing message
         * 4. Send result to outbound topic
         * 5. Print processed message
         */
        customerPolicies
                .leftJoin(claimPayments, (value1, value2) -> new CustomerView(
                        value1.getCollection1().stream().findFirst().map(CustomerMessage::getCustomer).orElse(null),
                        value1.getCollection1(),
                        value1.getCollection2(),
                        value2 == null ? null : value2.getCollection1(),
                        value2 == null ? null : value2.getCollection2()))
                .toStream((key, value) -> value.getCustomerKey())
                .through(PocConstants.CUSTOMER_VIEW_TOPIC, Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getSerde(CustomerView.class)));

        streamsBuilderUtil.start();
    }

    private String getPolicyNumber(String claimNumber) {
        return claimNumber.split("_")[0];
    }
}