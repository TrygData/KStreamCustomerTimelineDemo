package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
public class ClaimAndPayment {
    public ClaimList claimList = new ClaimList();
    public PaymentList paymentList = new PaymentList();



}
