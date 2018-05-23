package kstream.demo2;

public class CustomerAndPolicy {
    public CustomerList customerList = new CustomerList();
    public PolicyList policyList = new PolicyList();

    public CustomerAndPolicy() {
    }

    public CustomerAndPolicy(CustomerList customerList, PolicyList policyList) {
        this.customerList = customerList;
        this.policyList = policyList;
    }



}
