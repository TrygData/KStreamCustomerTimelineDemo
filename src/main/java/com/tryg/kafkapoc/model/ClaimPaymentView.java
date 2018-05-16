package com.tryg.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class ClaimPaymentView {
    private List<ClaimMessage> claims;
    private List<PaymentMessage> payments;
}
