package kstream.demo3;

public class CustomerAndPolicy {
    public CustomerList customerList = new CustomerList();
    public PolicyList policyList = new PolicyList();

    public CustomerAndPolicy() {
    }

    public CustomerAndPolicy(CustomerList customerList, PolicyList policyList) {
        this.customerList = customerList;
        this.policyList = policyList;
    }

    public String toString() {
        return "{" + customerList.toString() + "," + policyList.toString() + "}";
    }

}
