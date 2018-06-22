/** 
 *  @ CustomerPolicyClaimPayment.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;

import java.util.ArrayList;

/** 
 * Description Class stores all the joined aggregated data from the claim,payment.customer,claim payment
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class CustomerView {

	public CustomerView() {

	}

	public String cutomerKey;
	public ArrayList<Customer> customerRecords = new ArrayList<>();
	public ArrayList<Policy> policyRecords = new ArrayList<>();
	public ArrayList<Claim> claimRecords = new ArrayList<>();
	public ArrayList<ClaimPayment> paymentRecords = new ArrayList<>();

}
