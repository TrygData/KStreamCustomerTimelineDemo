package com.tryg.services;

import org.apache.kafka.streams.kstream.KTable;

import com.tryg.model.InputJsonModels.ClaimMessage;
import com.tryg.model.InputJsonModels.CustomerMessage;
import com.tryg.model.InputJsonModels.PaymentMessage;
import com.tryg.model.InputJsonModels.PolicyMessage;
import com.tryg.model.IntermediateJsonModel.ClaimAndPayment;
import com.tryg.model.IntermediateJsonModel.CustomerPolicyClaimPayment;
import com.tryg.model.OutputJsonModel.CustomerView;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ServiceClass for CustomerView
 */
public class CustomerViewService {

	public KTable<Integer, CustomerView> getCustomerViewData(
			KTable<Integer, CustomerPolicyClaimPayment> allJoinedAndCoGrouped) {
		return allJoinedAndCoGrouped.<CustomerView>mapValues((all) -> {
			CustomerView view = new CustomerView();
			view.cutomerKey = Integer.parseInt(
					all.customerAndPolicy.customerList.customerRecords.get(0).CUSTOMER.replaceFirst("cust", ""));
			for (CustomerMessage customer : all.customerAndPolicy.customerList.customerRecords) {
				view.customerRecords.add(customer);
			}
			for (PolicyMessage policy : all.customerAndPolicy.policyList.policyRecords) {
				view.policyRecords.add(policy);
			}
			if (all.claimAndPayment2 != null) {
				for (ClaimAndPayment claimAndPayment : all.claimAndPayment2.claimAndPaymentMap.values()) {

					for (ClaimMessage claim : claimAndPayment.claimList.claimRecords) {
						view.claimRecords.add(claim);
					}
					if (claimAndPayment.paymentList != null) {
						for (PaymentMessage payment : claimAndPayment.paymentList.paymentRecords) {
							view.paymentRecords.add(payment);
						}
					}
				}
			}
			return view;
		});
	}

}
