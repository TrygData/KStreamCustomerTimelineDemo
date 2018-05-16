package com.tryg.kafkapoc.model;

import lombok.Data;

@Data
public class PolicyMessage {
    private Integer policy;
    private Integer pVar0;
    private Integer pVar1;
    private String policyEndTime;
    private String policyStartTime;
}
