package com.tryg.kafkapoc.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class GenericListsView<V1, V2> {
    List<V1> list1;
    List<V2> list2;
}
