package kstream.demo3;

public class ClaimAndPayment {
    public ClaimList claimList = new ClaimList();
    public PaymentList paymentList = new PaymentList();

    public ClaimAndPayment() {
    }

    public ClaimAndPayment(ClaimList claimList, PaymentList paymentList) {
        this.claimList = claimList;
        this.paymentList = paymentList;
    }

    public String toString() {
        return "{" + claimList + "," + paymentList + "}";
    }

}
