package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.*;

@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
@Builder
public @Data
class PaymentMessage {
     public  Double PAYMENT;
    public Double PAYTIME;
    public Integer CLAIMCOUNTER;
    public String CLAIMNUMBER;
}
