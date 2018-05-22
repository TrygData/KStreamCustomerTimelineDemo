/** 
 *  @ CustomerPolicyClaimPayment.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;
/** 
 * Description  POJO Class stores complete data for all the topics joined
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class CustomerPolicyClaimPayment {

	public CustomerPolicyJoined customerAndPolicy = new CustomerPolicyJoined();
	public ClaimPaymentMap claimAndPayment2 = new ClaimPaymentMap();

	public CustomerPolicyClaimPayment(CustomerPolicyJoined customerAndPolicy, ClaimPaymentMap claimAndPayment2) {
		super();
		this.customerAndPolicy = customerAndPolicy;
		this.claimAndPayment2 = claimAndPayment2;
	}

}
