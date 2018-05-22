package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
public class CustomerPolicyClaimPayment {
    public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
    public ClaimAndPayment2 claimAndPayment2 = new ClaimAndPayment2();


}
