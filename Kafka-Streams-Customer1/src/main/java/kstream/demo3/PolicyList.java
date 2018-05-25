package kstream.demo3;

import java.util.ArrayList;

public class PolicyList {
    public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();

    public PolicyList() {
    }

    public String toString() {
        return policyRecords.toString();
    }
}
