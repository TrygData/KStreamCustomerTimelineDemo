package com.tryg.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GenericCollections<V1, V2> {
    Collection<V1> collection1;
    Collection<V2> collection2;
}
