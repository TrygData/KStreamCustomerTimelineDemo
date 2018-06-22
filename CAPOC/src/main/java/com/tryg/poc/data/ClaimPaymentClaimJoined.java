/** 
 *  @ ClaimPaymentClaimJoined.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;

import java.util.ArrayList;

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
	public ArrayList<ClaimPayment> claimPayment=new ArrayList<>();
	public ArrayList<Claim> claim=new ArrayList<>();

	public ClaimPaymentClaimJoined(ClaimPaymentList claimPaymentList, ClaimList claimList) {
		this.claimPaymentList = claimPaymentList;
		this.claimList = claimList;
	}

	public ClaimPaymentClaimJoined(ArrayList<ClaimPayment> claimPayment, ArrayList<Claim> claim) {
		this.claimPayment = claimPayment;
		this.claim = claim;
	}

}
