package com.tryg.kafkapoc.model;

import lombok.Data;

@Data
public class PaymentMessage {
    private Double payment;
    private Double payTime;
    private String claimNumber; // = <policy>_<claimCounter>
    private Integer claimCounter;
}