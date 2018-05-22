package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;



import lombok.*;

import java.util.ArrayList;
@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
    @Builder
@Data
public class PaymentList {

    public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

}
