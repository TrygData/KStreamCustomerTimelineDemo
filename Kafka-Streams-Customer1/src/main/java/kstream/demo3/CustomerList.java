package kstream.demo3;

import java.util.ArrayList;

public class CustomerList {
    public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
    String s = new String();

    public CustomerList() {
    }

    public String toString() {
        return customerRecords.toString();
    }
}
