package com.tryg.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class CustomerPolicyView {
    private List<CustomerMessage> customers;
    private List<PolicyMessage> policies;
}
