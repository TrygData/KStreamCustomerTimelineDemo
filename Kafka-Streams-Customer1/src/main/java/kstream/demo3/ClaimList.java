package kstream.demo3;

import java.util.ArrayList;

public class ClaimList {
    public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();

    public ClaimList() {
    }

    public String toString() {
        return this.claimRecords.toString();
    }
}
