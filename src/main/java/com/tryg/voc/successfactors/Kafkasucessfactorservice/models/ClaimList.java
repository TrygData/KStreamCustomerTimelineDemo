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
public class ClaimList {
    public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();


}
