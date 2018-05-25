package kstream.demo3;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class CustomerMessage {

    public String ADDRESS;
    public String CUSTOMER;
    public Double CUSTOMERTIME;
    public String POLICY;


    @JsonIgnore
    public int getPolicyAsInt() {
        return Integer.parseInt(this.POLICY);
    }

    public String toString() {
        return CUSTOMER + "/" + POLICY;
    }
}
