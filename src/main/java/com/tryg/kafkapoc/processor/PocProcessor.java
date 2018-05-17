package com.tryg.kafkapoc.processor;

import com.tryg.kafkapoc.config.KafkaPropertiesFactory;
import com.tryg.kafkapoc.config.PocConstants;
import com.tryg.kafkapoc.model.*;
import com.tryg.kafkapoc.serde.SerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Component
public class PocProcessor {

    private final KafkaPropertiesFactory propsFactory;
    private final SerdeFactory serdeFactory;

    @Autowired
    public PocProcessor(KafkaPropertiesFactory propsFactory, SerdeFactory serdeFactory) {
        this.propsFactory = propsFactory;
        this.serdeFactory = serdeFactory;
    }

    @PostConstruct
    public void process() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, CustomerMessage> customers = streamsBuilder.stream(PocConstants.CUSTOMER_TOPIC,
                Consumed.with(Serdes.String(), serdeFactory.getSerde(CustomerMessage.class)));

        KStream<Integer, PolicyMessage> policies = streamsBuilder.stream(PocConstants.POLICY_TOPIC,
                Consumed.with(Serdes.Integer(), serdeFactory.getSerde(PolicyMessage.class)));

        KStream<String, ClaimMessage> claims = streamsBuilder.stream(PocConstants.CLAIM_TOPIC,
                Consumed.with(Serdes.String(), serdeFactory.getSerde(ClaimMessage.class)));

        KStream<String, PaymentMessage> payments = streamsBuilder.stream(PocConstants.PAYMENT_TOPIC,
                Consumed.with(Serdes.String(), serdeFactory.getSerde(PaymentMessage.class)));

        KTable<String, List<CustomerMessage>> customerTable = groupAndAggregateInList(
                customers,
                String.class,
                CustomerMessage.class,
                CustomerMessage::getPolicy
        );

        KTable<String, List<PolicyMessage>> policyTable = groupAndAggregateInList(
                policies,
                String.class,
                PolicyMessage.class,
                m -> String.valueOf(m.getPolicy())
        );

        KTable<String, CustomerPolicyView> customerPolicies = customerTable.join(policyTable, CustomerPolicyView::new);

        KTable<String, List<ClaimMessage>> claimTable = groupAndAggregateInList(
                claims,
                String.class,
                ClaimMessage.class,
                m -> getPolicyNumber(m.getClaimNumber())
        );

        KTable<String, List<PaymentMessage>> paymentTable = groupAndAggregateInList(
                payments,
                String.class,
                PaymentMessage.class,
                m -> getPolicyNumber(m.getClaimNumber())
        );

        KTable<String, ClaimPaymentView> claimPayments = claimTable.join(paymentTable, ClaimPaymentView::new);

        KTable<String, CustomerView> customerViews = customerPolicies.join(claimPayments, (value1, value2) ->
                new CustomerView(value1.getCustomers().get(0).getCustomer(), value1.getCustomers(), value1.getPolicies(), value2.getClaims(), value2.getPayments()));

        KStream<String, CustomerView> customerOutStream = customerViews.toStream((key, value) -> value.getCustomerKey());

        customerOutStream.foreach((key, value) -> System.out.println("Processed message (" + key + ", " + value + ")"));

        customerOutStream.to(PocConstants.CUSTOMER_VIEW_TOPIC, Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getSerde(CustomerView.class)));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), propsFactory.getFullProperties());
        kafkaStreams.start();
    }

    private <V, K> KTable<K, List<V>> groupAndAggregateInList(KStream<?, V> stream, Class<K> keyClass, Class<V> valueClass, Function<V, K> keyFunc) {
        return stream
                .groupBy(
                        (key, value) -> keyFunc.apply(value),
                        Serialized.with(serdeFactory.getSerde(keyClass), serdeFactory.getSerde(valueClass)))
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        Materialized.with(serdeFactory.getSerde(keyClass), serdeFactory.getListSerde(valueClass)));
    }

    private String getPolicyNumber(String claimNumber) {
        return claimNumber.split("_")[0];
    }
}