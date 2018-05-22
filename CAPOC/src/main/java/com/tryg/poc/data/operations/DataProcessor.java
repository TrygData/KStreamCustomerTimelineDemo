
/** 
 *  @ DataProcessor.java v1.0   16/05/2018 
 *  
 *  */ 

package com.tryg.poc.data.operations;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;

import com.tryg.poc.data.Claim;
import com.tryg.poc.data.ClaimList;
import com.tryg.poc.data.ClaimPayment;
import com.tryg.poc.data.ClaimPaymentClaimJoined;
import com.tryg.poc.data.ClaimPaymentList;
import com.tryg.poc.data.ClaimPaymentMap;
import com.tryg.poc.data.Customer;
import com.tryg.poc.data.CustomerList;
import com.tryg.poc.data.CustomerPolicyClaimPayment;
import com.tryg.poc.data.CustomerPolicyJoined;
import com.tryg.poc.data.CustomerView;
import com.tryg.poc.data.Policy;
import com.tryg.poc.data.PolicyList;
import com.tryg.poc.util.Constants;
import com.tryg.poc.util.JSONSerdeUtils;
import com.tryg.poc.util.KstreamUtil;

/** 
 * Description Class defines processing logic of Kstream data
* @author Joseph James 
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 

public class DataProcessor {

	KstreamUtil obKstreamUtil=new KstreamUtil();

	/**
	 * method to process kafka stream data from different topics 
	 * @param
	 * @return 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
	public void processData() throws IllegalAccessException, InstantiationException
	{
		KStreamBuilder builder=obKstreamUtil.getKKStreamBuilder();
		
		/** create KStream from different data topics*/
		
		KStream<String, Customer> customerKStream=obKstreamUtil.getStringKeyKstream(builder, Constants.customerTopic, Customer.class);
		KStream<String, Policy> policyKStream=obKstreamUtil.getStringKeyKstream(builder, Constants.PolicyTopic, Policy.class);
		KStream<String, Claim> claimStream = obKstreamUtil.getStringKeyKstream(builder, Constants.claimTopic, Claim.class);
		KStream<String, ClaimPayment> ClaimPaymentKStream = obKstreamUtil.getStringKeyKstream(builder, Constants.claimPaymentTopic, 
				ClaimPayment.class);
		
		/**create KTables from the filtered and grouped data from different topics*/
		
		KTable<String, ClaimList> claimStrGroupedData =aggregateClaimGrupedData(claimStream.filter(claimValidate()));
		KTable<String, ClaimPaymentList> paymentGroupedData=aggregateClaimPaymentGroupedData(ClaimPaymentKStream.filter(claimpaymentValidate()));
		KTable<Integer, CustomerList> customerGroupedData=aggregateCustomerGroupedData(customerKStream.filter(customerValidate()));
		KTable<Integer, PolicyList> policyGroupedData =aggregatePolicyGrupedData(policyKStream.filter(policyValidate()));
		
		/**Join data from aggregated Claim topic and ClaimPayment topic*/
		
		KTable<String, ClaimPaymentClaimJoined> claimpaymentandClainmGrouped = claimStrGroupedData
				.leftJoin(paymentGroupedData, (claim, payment) -> new ClaimPaymentClaimJoined(payment, claim));
		KTable<Integer, CustomerPolicyJoined> customerAndPolicyGroupedKTable = customerGroupedData
				.join(policyGroupedData, (customer, policy) -> new CustomerPolicyJoined(customer, policy));
		KTable<Integer, ClaimPaymentMap> claimAndPayment2IntGroupedTable=retriveKeyAggregateData(claimpaymentandClainmGrouped.toStream());
		KTable<Integer, CustomerPolicyClaimPayment> finalData = customerAndPolicyGroupedKTable.leftJoin(
				claimAndPayment2IntGroupedTable, (left, right) -> new CustomerPolicyClaimPayment(left, right));
		
		/**Create complete view for the final data joined from different topics*/
		
		KTable<Integer, CustomerView> customerView  =createDataView(finalData);
		customerView.print();
		customerView.through(Constants.integerSerde, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(CustomerView.class),
		JSONSerdeUtils.getJSONDeSerializer(CustomerView.class)), Constants.CUSTOMER_VIEW_OUT);
		
		/**start the kafka streams*/
		 
		obKstreamUtil.startKstreams(builder);
	}
	
	/**
	 * create KTable from Grouped aggregated Data for Claim and Claimpayment
	 * @param stream
	 * @return KTable
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	
	private KTable<Integer, ClaimPaymentMap> retriveKeyAggregateData(KStream<String, ClaimPaymentClaimJoined> stream) throws IllegalAccessException, InstantiationException {
		return stream.groupBy((k, claimPay) -> (claimPay != null)? Integer.parseInt(claimPay.claimList.claimRecords.get(0).
		CLAIMNUMBER.split("_")[0]): 999, Constants.integerSerde, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(ClaimPaymentClaimJoined.class),
		JSONSerdeUtils.getJSONDeSerializer(ClaimPaymentClaimJoined.class)))
		.aggregate(ClaimPaymentMap::new, (claimKey, claimPay, claimAndPay2) -> {
				claimAndPay2.add(claimPay);
				return claimAndPay2;
		}, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(ClaimPaymentList.class),
		JSONSerdeUtils.getJSONDeSerializer(ClaimPaymentMap.class)), Constants.CLAIM_AND_PAYMENT_STORE);
	}
	
	/**
	 * create KTable from Grouped aggregated Data for Policy
	 * @param policyKStream
	 * @return KTable
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	
	private KTable<Integer, PolicyList> aggregatePolicyGrupedData(KStream<String, Policy> policyKStream) throws IllegalAccessException, InstantiationException {
		return policyKStream.groupBy((k, policy) -> Integer.parseInt(policy.POLICY), Constants.integerSerde, 
		JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(Policy.class),	JSONSerdeUtils.getJSONDeSerializer(Policy.class)))
		.aggregate(PolicyList::new, (policyKey, policy, policyLst) -> {
				policyLst.PolicyRecords.add(policy);
				return (policyLst);
		}, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(PolicyList.class),
		JSONSerdeUtils.getJSONDeSerializer(PolicyList.class)), Constants.POLICY_STORE);
	}
	
	/**
	 * create KTable from Grouped aggregated Data from Policy
	 * @param customerKStream
	 * @return KTable
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	
	private KTable<Integer, CustomerList> aggregateCustomerGroupedData(KStream<String, Customer> customerKStream) throws IllegalAccessException, InstantiationException {
		return customerKStream.groupBy((key, value) -> Integer.parseInt(value.POLICY), Constants.integerSerde, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(Customer.class),
		JSONSerdeUtils.getJSONDeSerializer(Customer.class))).aggregate(CustomerList::new, (ckey, custMessage, customerList) -> {
			customerList.CustomerRecords.add(custMessage);
			return customerList;
		}, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(CustomerList.class),
		JSONSerdeUtils.getJSONDeSerializer(CustomerList.class)), Constants.CUSTOMER_STORE);
	}

	/**
	 * create KTable from Grouped aggregated Data from ClaimPayment
	 * @param claimPaymentKStream
	 * @return KTable
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */

	private KTable<String, ClaimPaymentList> aggregateClaimPaymentGroupedData(KStream<String, ClaimPayment> claimPaymentKStream) throws IllegalAccessException, InstantiationException {
		return claimPaymentKStream.groupBy((k, payment) -> payment.CLAIMNUMBER, Constants.stringSerde, 
		JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(ClaimPayment.class),JSONSerdeUtils.getJSONDeSerializer(ClaimPayment.class)))
		.aggregate(ClaimPaymentList::new, (ClaimPaykey, ClaimPay, claimpayList) -> {
				claimpayList.ClaimPaymentRecord.add(ClaimPay);
				return claimpayList;
		}, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(ClaimPaymentList.class),
		JSONSerdeUtils.getJSONDeSerializer(ClaimPaymentList.class)), Constants.PAYMENT_STORE);
		
	}

	/**
	 * create KTable from Grouped aggregated Data from Claim
	 * @param claimStream
	 * @return KTable
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	
	private KTable<String, ClaimList> aggregateClaimGrupedData(KStream<String, Claim> claimStream) throws IllegalAccessException, InstantiationException {
		return claimStream.groupBy((k, claim) -> claim.CLAIMNUMBER, Constants.stringSerde, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(Claim.class),
		JSONSerdeUtils.getJSONDeSerializer(Claim.class))).aggregate(ClaimList::new, (claimKey, claimMsg, claimLst) -> {
			claimLst.claimRecords.add(claimMsg); return (claimLst);
		}, JSONSerdeUtils.gerJsonSerde(JSONSerdeUtils.getJSONSerializer(ClaimList.class), JSONSerdeUtils.getJSONDeSerializer(ClaimList.class)), Constants.CLAIM_STORE);
		 
	}
	
	/**
	 * Get and properly arrange the Customer data 
	 * @param KTable finalData
	 * @return KTable 
	 */
	
	private KTable<Integer, CustomerView> createDataView(KTable<Integer, CustomerPolicyClaimPayment> finalData) {
		
		return finalData.<CustomerView>mapValues((all) -> {
			CustomerView view = new CustomerView();
			view.cutomerKey = Integer.parseInt(	all.customerAndPolicy.customerList.CustomerRecords.get(0).CUSTOMER.
			replaceFirst("cust", ""));
			for (Customer customer : all.customerAndPolicy.customerList.CustomerRecords) {
				view.customerRecords.add(customer);
			}
			for (Policy policy : all.customerAndPolicy.policyList.PolicyRecords) {
				view.policyRecords.add(policy);
			}
			if (all.claimAndPayment2 != null) {
				for (ClaimPaymentClaimJoined claimAndPayment : all.claimAndPayment2.claimAndPaymentMap.values()) {

					for (Claim claim : claimAndPayment.claimList.claimRecords) {
						view.claimRecords.add(claim);
					}
					if (claimAndPayment.claimPaymentList != null) {
						for (ClaimPayment payment : claimAndPayment.claimPaymentList.ClaimPaymentRecord) {
							view.paymentRecords.add(payment);
						}
					}
				}
			}
			return view;
		});

	}

	/**
	 * @param 
	 * @return predicate to validate the Customer records
	 */
	
	public  Predicate<String,Customer> customerValidate() {
		 	return new Predicate<String, Customer>() {
				@Override
				public boolean test(String key, Customer value) {
					return (value!=null)&&(value.POLICY!= null) && (value.CUSTOMER!=null);
				}
			};
	}
	
	/**
	 * @param 
	 * @return predicate to validate the Claim records
	 */
	
	public  Predicate<String,Claim> claimValidate() {
		 	return new Predicate<String, Claim>() {
				@Override
				public boolean test(String key, Claim value) {
					return (value.CLAIMNUMBER!= null) && (value.CLAIMCOUNTER!=null);
				}
			};
	}
	
	/***
	 * @param
	 * @return predicate to validate the ClaimPayment records
	 */
	
	public  Predicate<String, ClaimPayment> claimpaymentValidate() {
	 	return new Predicate<String, ClaimPayment>() {
			@Override
			public boolean test(String key, ClaimPayment value) {
				return (value.CLAIMCOUNTER!= null) && (value.CLAIMCOUNTER!=null);
			}
		};
	}
	
	/***
	 * @param
	 * @return predicate to validate the Policy records
	 */
	
	public  Predicate<String, Policy> policyValidate() {
	 	return new Predicate<String, Policy>() {
			@Override
			public boolean test(String key, Policy value) {
				return value.POLICY!= null ;
			}
		};
	}
	
}
