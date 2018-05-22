/** 
 *  @ ClaimPaymentMap.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;

import java.util.HashMap;
import java.util.Map;
/** 
 * Description  Class stored map  of Claimpayment and Claim joined data
* @author   
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class ClaimPaymentMap {
	public Map<String, ClaimPaymentClaimJoined> claimAndPaymentMap = new HashMap<>();

	public void add(final ClaimPaymentClaimJoined claimAndPaymentMessage) {
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
