/** 
 *  @ ClaimPaymentClaimJoined.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;

/** 
 * Description   class store List of ClaimPayemnt and Claim 
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class ClaimPaymentClaimJoined {

	public ClaimPaymentClaimJoined() {
		super();
		
	}

	public ClaimPaymentList claimPaymentList = new ClaimPaymentList();
	public ClaimList claimList = new ClaimList();

	public ClaimPaymentClaimJoined(ClaimPaymentList claimPaymentList, ClaimList claimList) {
		this.claimPaymentList = claimPaymentList;
		this.claimList = claimList;
	}

}
