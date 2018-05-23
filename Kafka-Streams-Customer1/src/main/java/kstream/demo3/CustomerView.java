package main.java.kstream.demo3;

import java.util.ArrayList;

public class CustomerView {

    public int cutomerKey;
    public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
    public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();
    public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();
    public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

    public CustomerView() {
    }

}
