package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.*;

@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
@Data
public class CustomerAndPolicy {

    private CustomerList customerList = new CustomerList();
    private PolicyList policyList = new PolicyList();



}
