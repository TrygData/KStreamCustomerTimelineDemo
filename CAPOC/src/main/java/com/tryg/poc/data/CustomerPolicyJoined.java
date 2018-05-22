/** 
 *  @ CustomerPolicyJoined.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;
/** 
 * Description  POJO Class stores  list of joined data for customer and policy
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class CustomerPolicyJoined {

	public CustomerList customerList = new CustomerList();
	public PolicyList policyList = new PolicyList();

	public CustomerPolicyJoined(CustomerList customerList, PolicyList policyList) {

		this.customerList = customerList;
		this.policyList = policyList;
	}

	public CustomerPolicyJoined() {

	}

}
