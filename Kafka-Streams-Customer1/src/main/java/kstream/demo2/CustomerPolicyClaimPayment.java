package kstream.demo2;

public class CustomerPolicyClaimPayment {
    public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
    public ClaimAndPayment2 claimAndPayment2 = new ClaimAndPayment2();

    public CustomerPolicyClaimPayment() {
    }

    public CustomerPolicyClaimPayment(CustomerAndPolicy customerAndPolicy, ClaimAndPayment2 claimAndPayment2) {
        this.customerAndPolicy = customerAndPolicy;
        this.claimAndPayment2 = claimAndPayment2;
    }

}
