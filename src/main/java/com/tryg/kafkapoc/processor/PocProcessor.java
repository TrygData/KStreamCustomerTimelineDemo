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

        KTable<String, GenericDoubleListView<CustomerMessage, PolicyMessage>> customerPolicies = joinTwoStreams(
                customers,
                policies,
                String.class,
                CustomerMessage.class,
                PolicyMessage.class,
                CustomerMessage::getPolicy,
                policyMessage -> String.valueOf(policyMessage.getPolicy())
        );

        KTable<String, GenericDoubleListView<ClaimMessage, PaymentMessage>> claimPayments = joinTwoStreams(
                claims,
                payments,
                String.class,
                ClaimMessage.class,
                PaymentMessage.class,
                claimMessage -> getPolicyNumber(claimMessage.getClaimNumber()),
                paymentMessage -> getPolicyNumber(paymentMessage.getClaimNumber())
        );

        KTable<String, CustomerView> customerViews = customerPolicies.join(claimPayments, (value1, value2) ->
                new CustomerView(value1.getList1().get(0).getCustomer(), value1.getList1(), value1.getList2(), value2.getList1(), value2.getList2()));

        KStream<String, CustomerView> customerOutStream = customerViews.toStream((key, value) -> value.getCustomerKey());

        customerOutStream.foreach((key, value) -> System.out.println("Processed message (" + key + ", " + value + ")"));

        customerOutStream.to(PocConstants.CUSTOMER_VIEW_TOPIC, Produced.with(serdeFactory.getSerde(String.class), serdeFactory.getSerde(CustomerView.class)));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), propsFactory.getFullProperties());
        kafkaStreams.start();
    }

    private <K, V1, V2> KTable<K, GenericDoubleListView<V1, V2>> joinTwoStreams(KStream<?, V1> stream1, KStream<?, V2> stream2,
                                                                                Class<K> keyClass, Class<V1> valueClass1, Class<V2> valueClass2,
                                                                                Function<V1, K> keyFunc1, Function<V2, K> keyFunc2) {
        KTable<K, List<V1>> table1 = groupAndAggregateInList(stream1, keyClass, valueClass1, keyFunc1);
        KTable<K, List<V2>> table2 = groupAndAggregateInList(stream2, keyClass, valueClass2, keyFunc2);

        return table1.join(table2, GenericDoubleListView::new);
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