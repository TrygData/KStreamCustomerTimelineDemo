package com.tryg.kafkapoc.processor;

import com.tryg.kafkapoc.config.PocConstants;
import com.tryg.kafkapoc.config.StreamsBuilderUtil;
import com.tryg.kafkapoc.model.*;
import com.tryg.kafkapoc.serde.SerdeFactory;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
public class PocProcessor {

    private final SerdeFactory serdeFactory;
    private final StreamsBuilderUtil streamsBuilderUtil;

    @Autowired
    public PocProcessor(SerdeFactory serdeFactory, StreamsBuilderUtil streamsBuilderUtil) {
        this.serdeFactory = serdeFactory;
        this.streamsBuilderUtil = streamsBuilderUtil;
    }

    @PostConstruct
    public void process() {
        KStream<String, CustomerMessage> customers = streamsBuilderUtil.readTopic(PocConstants.CUSTOMER_TOPIC, String.class, CustomerMessage.class);
        KStream<Integer, PolicyMessage> policies = streamsBuilderUtil.readTopic(PocConstants.POLICY_TOPIC, Integer.class, PolicyMessage.class);
        KStream<String, ClaimMessage> claims = streamsBuilderUtil.readTopic(PocConstants.CLAIM_TOPIC, String.class, ClaimMessage.class);
        KStream<String, PaymentMessage> payments = streamsBuilderUtil.readTopic(PocConstants.PAYMENT_TOPIC, String.class, PaymentMessage.class);

        payments.print(Printed.toSysOut());

        // Group all tables by policy number
        KTable<String, List<CustomerMessage>> groupedCustomers = streamsBuilderUtil.groupAndAggregateInList(customers, String.class, CustomerMessage.class, CustomerMessage::getPolicy);
        KTable<String, List<PolicyMessage>> groupedPolicies = streamsBuilderUtil.groupAndAggregateInList(policies, String.class, PolicyMessage.class, policyMessage -> String.valueOf(policyMessage.getPolicy()));
        KTable<String, List<ClaimMessage>> groupedClaims = streamsBuilderUtil.groupAndAggregateInList(claims, String.class, ClaimMessage.class, claimMessage -> getPolicyNumber(claimMessage.getClaimNumber()));
        KTable<String, List<PaymentMessage>> groupedPayments = streamsBuilderUtil.groupAndAggregateInList(payments, String.class, PaymentMessage.class, paymentMessage -> getPolicyNumber(paymentMessage.getClaimNumber()));

        //Join customers with policies and claims with payments
        KTable<String, GenericListsView<CustomerMessage, PolicyMessage>> customerPolicies = groupedCustomers.join(groupedPolicies, GenericListsView::new);
        KTable<String, GenericListsView<ClaimMessage, PaymentMessage>> claimPayments = groupedClaims.join(groupedPayments, GenericListsView::new);

        /*
         * 1. Join customer-policy and claim-payment streams on policy number into CustomerView
         * 2. Map to stream and change key to customer
         * 3. Print processing message
         * 4. Send result to outbound topic
         * 5. Print processed message
         */
        customerPolicies
                .join(claimPayments, (value1, value2) -> new CustomerView(value1.getList1().get(0).getCustomer(), value1.getList1(), value1.getList2(), value2.getList1(), value2.getList2()))
                .toStream((key, value) -> value.getCustomerKey())
                .peek((key, value) -> System.out.println("Processing message (" + key + ", " + value + ")"))
                .through(PocConstants.CUSTOMER_VIEW_TOPIC, Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getSerde(CustomerView.class)));

        streamsBuilderUtil.start();
    }

    private String getPolicyNumber(String claimNumber) {
        return claimNumber.split("_")[0];
    }
}