package com.tryg.voc.successfactors.Kafkasucessfactorservice;


import com.tryg.voc.successfactors.Kafkasucessfactorservice.models.*;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.apache.kafka.streams.kstream.Serialized;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;


@Component
@EnableBinding(PaymentStream.class)
public class StreamProcessorComponent {


    private SerdesFactory serdesFactory;


    private Serde<String> stringSerde = new Serdes.StringSerde();

    private Serde<Integer> integerSerde = new Serdes.IntegerSerde();

    public StreamProcessorComponent(SerdesFactory serdesFactory) {
        this.serdesFactory = serdesFactory;
    }

    @StreamListener

    public void payment(@Input(PaymentStream.CLAIM) KStream<String, ClaimMessage> stringClaimMessageKStream,
                        @Input(PaymentStream.PAYMENT) KStream<String, PaymentMessage> paymentStream,
                        @Input(PaymentStream.CUSTOMER) KStream<String, CustomerMessage> customerMessageKStream,
                        @Input(PaymentStream.POLICY) KStream<String, PolicyMessage> policyMessageKStream
    ) {
        KTable<String, ClaimList> stringClaimMessageKTable = stringClaimMessageKStream.groupBy((s, claimMessage) -> claimMessage.CLAIMNUMBER, Serialized.with(stringSerde, (Serde<ClaimMessage>) serdesFactory.serdes(ClaimMessage.class))).aggregate(ClaimList::new, (s, claimMessage, claimList) -> {
            claimList.claimRecords.add(claimMessage);
            return claimList;

        });

        KTable<String, PaymentList> paymentListKTable = paymentStream.groupBy((s, paymentMessage) -> paymentMessage.CLAIMNUMBER, Serialized.with(stringSerde, (Serde<PaymentMessage>) serdesFactory.serdes(PaymentMessage.class))).aggregate(PaymentList::new, (aDouble, paymentMessage, paymentList) -> {
            paymentList.paymentRecords.add(paymentMessage);
            return paymentList;

        });


        KTable<Integer, CustomerList> customerListKTable = customerMessageKStream.groupBy((s, customerMessage) -> Integer.valueOf(customerMessage.POLICY),
                Serialized.with(integerSerde, (Serde<CustomerMessage>) serdesFactory.serdes(CustomerMessage.class))).aggregate(CustomerList::new, (s, customerMessage, customerList) -> {
            customerList.customerRecords.add(customerMessage);
            return customerList;

        });

        KTable<Integer, PolicyList> policyListKTable = policyMessageKStream.groupBy((integer, policyMessage) -> policyMessage.POLICY,
                Serialized.with(integerSerde, (Serde<PolicyMessage>) serdesFactory.serdes(PolicyMessage.class))).aggregate(PolicyList::new, (integer, policyMessage, policyList) -> {
            policyList.policyRecords.add(policyMessage);
            return policyList;
        });
//
        KTable<Integer, CustomerAndPolicy>  stringCustomerAndPolicyKTable = customerListKTable.join(policyListKTable, (customerList, policyList) -> new CustomerAndPolicy(customerList, policyList));

        KStream<String, ClaimAndPayment> stringClaimAndPaymentKStream = stringClaimMessageKTable.join(paymentListKTable, (claimList, paymentList) -> new ClaimAndPayment(claimList, paymentList)).toStream();

        KTable<Integer, ClaimAndPayment2> stringClaimAndPayment2KTable = stringClaimAndPaymentKStream.groupBy((s, claimAndPayment) -> sort(claimAndPayment), Serialized.with(integerSerde, (Serde<ClaimAndPayment>) serdesFactory.serdes(ClaimAndPayment.class))).aggregate(ClaimAndPayment2::new, (s, claimAndPayment, claimAndPayment2) -> {
            claimAndPayment2.add(claimAndPayment);
            return claimAndPayment2;
        });
    KTable<Integer, CustomerPolicyClaimPayment> stringCustomerPolicyClaimPaymentKTable = stringClaimAndPayment2KTable.join(stringCustomerAndPolicyKTable, (claimAndPayment2, customerAndPolicy) -> new CustomerPolicyClaimPayment(customerAndPolicy, claimAndPayment2));


    }


    private static int sort(ClaimAndPayment claimPay) {
        return (claimPay == null) ? 999 : Integer.parseInt(claimPay.claimList.claimRecords.get(0).CLAIMNUMBER.split("_")[0]);
    }

}
