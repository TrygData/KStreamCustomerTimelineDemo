package kstream.demo3;

import java.util.ArrayList;

public class PaymentList {
    public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

    public PaymentList() {
    }

    public String toString() {
        return this.paymentRecords.toString();
    }
}
