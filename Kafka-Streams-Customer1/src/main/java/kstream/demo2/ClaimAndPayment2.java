package kstream.demo2;

import java.util.HashMap;
import java.util.Map;

public class ClaimAndPayment2 {
    public Map<String, ClaimAndPayment> claimAndPaymentMap = new HashMap<>();

    public ClaimAndPayment2() {
    }

    public void add(final ClaimAndPayment claimAndPaymentMessage) {
        String key = claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMNUMBER
                + claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMTIME.toString();
        if (claimAndPaymentMap.containsKey(key)) {
            remove(key);
        }
        claimAndPaymentMap.put(key, claimAndPaymentMessage);
    }

    private void remove(final String key) {
        claimAndPaymentMap.remove(key);
    }

}
