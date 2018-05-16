package com.tryg.kafkapoc.model;

import lombok.Data;

@Data
public class CustomerMessage {
    private String address;
    private String customer;
    private Double customerTime;
    private String policy;
}
