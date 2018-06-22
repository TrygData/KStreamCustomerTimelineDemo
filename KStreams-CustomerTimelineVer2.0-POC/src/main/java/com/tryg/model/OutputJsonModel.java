package com.tryg.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.tryg.model.InputJsonModels.ClaimMessage;
import com.tryg.model.InputJsonModels.CustomerMessage;
import com.tryg.model.InputJsonModels.PaymentMessage;
import com.tryg.model.InputJsonModels.PolicyMessage;
import com.tryg.model.IntermediateJsonModel.ClaimAndPayment;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ModelClass for OutputJson
 */
public class OutputJsonModel {
	
	static public class ClaimAndPayment2 {
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
	
	
	static public class CustomerView {
		public int cutomerKey;
		public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
		public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();
		public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();
		public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

		public CustomerView() {
		}
	}
	

}
