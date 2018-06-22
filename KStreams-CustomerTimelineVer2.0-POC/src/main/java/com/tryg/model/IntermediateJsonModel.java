package com.tryg.model;

import com.tryg.model.InputJsonModels.ClaimList;
import com.tryg.model.InputJsonModels.CustomerList;
import com.tryg.model.InputJsonModels.PaymentList;
import com.tryg.model.InputJsonModels.PolicyList;
import com.tryg.model.OutputJsonModel.ClaimAndPayment2;


/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ModelClass for InputJson
 */
public class IntermediateJsonModel {
	
	static public class CustomerAndPolicy {
		public CustomerList customerList = new CustomerList();
		public PolicyList policyList = new PolicyList();

		public CustomerAndPolicy() {
		}

		public CustomerAndPolicy(CustomerList customerList, PolicyList policyList) {
			this.customerList = customerList;
			this.policyList = policyList;
		}

	}

	static public class ClaimAndPayment {
		public ClaimList claimList = new ClaimList();
		public PaymentList paymentList = new PaymentList();

		public ClaimAndPayment() {
		}

		public ClaimAndPayment(ClaimList claimList, PaymentList paymentList) {
			this.claimList = claimList;
			this.paymentList = paymentList;
		}
	}
	
	static public class CustomerPolicyClaimPayment {
		public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
		public ClaimAndPayment2 claimAndPayment2 = new ClaimAndPayment2();

		public CustomerPolicyClaimPayment() {
		}

		public CustomerPolicyClaimPayment(CustomerAndPolicy customerAndPolicy, ClaimAndPayment2 claimAndPayment2) {
			this.customerAndPolicy = customerAndPolicy;
			this.claimAndPayment2 = claimAndPayment2;
		}
	}

}
