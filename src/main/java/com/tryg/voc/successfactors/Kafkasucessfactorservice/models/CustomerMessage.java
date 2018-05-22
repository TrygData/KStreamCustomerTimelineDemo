package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
public class CustomerMessage {
    public String ADDRESS;
    public String CUSTOMER;
    public Double CUSTOMERTIME;
    public String POLICY;
}
