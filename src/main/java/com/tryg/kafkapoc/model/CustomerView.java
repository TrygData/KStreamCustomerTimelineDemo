package com.tryg.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class CustomerView {
    private String customerKey;

    private List<CustomerMessage> customers;
    private List<PolicyMessage> policies;
    private List<ClaimMessage> claims;
    private List<PaymentMessage> payments;
}
