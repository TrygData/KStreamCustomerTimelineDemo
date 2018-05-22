package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
public class ClaimAndPayment2 {
    public Map<String, ClaimAndPayment> claimAndPaymentMap = new HashMap<>();



    public void add(final ClaimAndPayment claimAndPaymentMessage) {
        String key = claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMNUMBER
                + claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMTIME.toString();
        if (claimAndPaymentMap.containsKey(key)) {
            remove(key);
        }
        claimAndPaymentMap.put(key, claimAndPaymentMessage);
    }

    private void remove(final String key) {
        claimAndPaymentMap.remove(key);
    }
}
