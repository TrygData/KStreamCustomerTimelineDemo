package com.tryg.kafkapoc.model;

import lombok.Data;

@Data
public class ClaimMessage {
    private String claimNumber; // = <policy>_<claimCounter>
    private Double claimTime;
    private Double claimReportTime;
    private String claimCounter;
}