package com.tryg.voc.successfactors.Kafkasucessfactorservice.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
@AllArgsConstructor

@NoArgsConstructor
@Getter
@Setter
public class CustomerList {
    public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
    String s = new String();


}
