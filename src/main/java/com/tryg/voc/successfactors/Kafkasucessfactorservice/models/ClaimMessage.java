package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
public class ClaimMessage {
    public Double CLAIMTIME;
    public String CLAIMNUMBER;
    public Double CLAIMREPORTTIME;
    public String CLAIMCOUNTER;
}
