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

        KTable<String, CustomerMessageList> customerTable = groupAndAggregateInList(
                customers,
                CustomerMessage::getPolicy,
                String.class,
                CustomerMessage.class,
                CustomerMessageList.class,
                CustomerMessageList::new
        );

        KTable<String, PolicyMessageList> policyTable = groupAndAggregateInList(
                policies,
                m -> String.valueOf(m.getPolicy()),
                String.class,
                PolicyMessage.class,
                PolicyMessageList.class,
                PolicyMessageList::new
        );

        KTable<String, CustomerPolicyView> customerPolicies = customerTable.join(policyTable, CustomerPolicyView::new);

        KTable<String, ClaimMessageList> claimTable = groupAndAggregateInList(
                claims,
                m -> getPolicyNumber(m.getClaimNumber()),
                String.class,
                ClaimMessage.class,
                ClaimMessageList.class,
                ClaimMessageList::new
        );

        KTable<String, PaymentMessageList> paymentTable = groupAndAggregateInList(
                payments,
                m -> getPolicyNumber(m.getClaimNumber()),
                String.class,
                PaymentMessage.class,
                PaymentMessageList.class,
                PaymentMessageList::new
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

    private <V, K, L extends List<V>> KTable<K, L> groupAndAggregateInList(KStream<?, V> stream, Function<V, K> groupByKeyFunction,
                                                                           Class<K> keyClass, Class<V> valueClass, Class<L> listClass,
                                                                           Initializer<L> listInitializer) {
        return stream
                .groupBy((key, value) -> groupByKeyFunction.apply(value),
                        Serialized.with(serdeFactory.getSerde(keyClass), serdeFactory.getSerde(valueClass)))
                .aggregate(listInitializer,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        Materialized.with(serdeFactory.getSerde(keyClass), serdeFactory.getSerde(listClass))
                );
    }

    private String getPolicyNumber(String claimNumber) {
        return claimNumber.split("_")[0];
    }
}