package kstream.demo3;

public class PaymentMessage {
    public Double PAYMENT;
    public Double PAYTIME;
    public Integer CLAIMCOUNTER;
    public String CLAIMNUMBER;


    public String toString() {
        return CLAIMNUMBER + "/" + PAYMENT;
    }
}
