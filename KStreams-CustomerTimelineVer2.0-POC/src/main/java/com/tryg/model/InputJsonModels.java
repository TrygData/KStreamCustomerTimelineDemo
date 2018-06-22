package com.tryg.model;

import java.util.ArrayList;
/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ModelClass for InputJson
 */

public class InputJsonModels {
	
	static public class CustomerMessage {
		public String ADDRESS;
		public String CUSTOMER;
		public Double CUSTOMERTIME;
		public String POLICY;
		
	}

	static public class PolicyMessage {
		public int PVAR1;
		public String POLICYENDTIME;
		public int POLICY;
		public String POLICYSTARTTIME;
		public int PVAR0;
		
		}
	

	static public class ClaimMessage {
		public Double CLAIMTIME;
		public String CLAIMNUMBER;
		public Double CLAIMREPORTTIME;
		public String CLAIMCOUNTER;
		
		}

	static public class PaymentMessage {
		public Double PAYMENT;
		public Double PAYTIME;
		public Integer CLAIMCOUNTER;
		public String CLAIMNUMBER;
		
		}
	
	
	
	static public class CustomerList {
		public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
		String s = new String();
	}

	static public class PolicyList {
		public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();

		public PolicyList() {
		}
	}
	
	
	static public class ClaimList {
		public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();

		public ClaimList() {
		}
	}
	
	static public class PaymentList {
		public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

		public PaymentList() {
		}
	}

}
