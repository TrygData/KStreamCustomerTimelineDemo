package com.tryg.voc.successfactors.Kafkasucessfactorservice;

import com.tryg.voc.successfactors.Kafkasucessfactorservice.models.*;

import org.apache.kafka.streams.kstream.KStream;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;



public interface PaymentStream {

    String CLAIM ="claim";
    String PAYMENT = "payment";
String POLICY = "policy";



String CUSTOMER = "customer";

    @Input(PAYMENT)
    KStream<String, PaymentMessage> paymentStream();


    @Input(POLICY)
    KStream<Integer, PolicyList> policyStream();
//
//
//
//
    @Input(CUSTOMER)
    KStream<String, CustomerMessage> streamCustomer();


    @Input(CLAIM)
    KStream<String, ClaimMessage> claimStreamMessage();






}
