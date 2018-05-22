package com.tryg.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerView {

    private String customerKey;

    private Collection<CustomerMessage> customers;
    private Collection<PolicyMessage> policies;
    private Collection<ClaimMessage> claims;
    private Collection<PaymentMessage> payments;
}
