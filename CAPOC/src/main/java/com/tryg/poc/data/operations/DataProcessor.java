
/** 
 *  @ DataProcessor.java v1.0   16/05/2018 
 *  
 *  */ 

package com.tryg.poc.data.operations;

import java.util.ArrayList;
import java.util.function.Function;


import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
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
import com.tryg.poc.util.ArrayListSerde;
import com.tryg.poc.util.Constants;
import com.tryg.poc.util.SerdeUtils;
import com.tryg.poc.util.KstreamUtil;

/** 
 * Description Class defines processing logic of Kstream data
* @author Joseph James 
* @version 1.0
* @param <T>
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 

public class DataProcessor<T> {

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
		
		KStream<String, Customer> customerKStream1=obKstreamUtil.getKstream(builder, Constants.customerTopic, Customer.class,String.class);
		KStream<String, Policy> policyKStream1=obKstreamUtil.getKstream(builder, Constants.PolicyTopic, Policy.class,String.class);
		KStream<String, Claim> claimStream1 = obKstreamUtil.getKstream(builder, Constants.claimTopic, Claim.class,String.class);
		KStream<String, ClaimPayment> ClaimPaymentKStream1 = obKstreamUtil.getKstream(builder, Constants.claimPaymentTopic,ClaimPayment.class,String.class);
		
	
		/**Customized function 	to get specific records*/	
		Function<Claim, String> claimFunction = Claim:: getCLAIMNUMBER; 
		Function<ClaimPayment, String> ClaimPaymentFunction = ClaimPayment:: getCLAIMNUMBER;
		Function<Customer, String> CustomerFunction = Customer:: getPOLICY;
		Function<Policy, String> policyFunction = Policy:: getPOLICY ;
		
		/**create KTables from grouped data from different topics*/
		
		KTable<String, ArrayList<Claim>> claimStrGroupedData1 = groupAndAggregateItems(claimStream1,String.class,Claim.class,claimFunction);
		KTable<String, ArrayList<ClaimPayment>> paymentGroupedData1 = groupAndAggregateItems(ClaimPaymentKStream1,String.class,ClaimPayment.class,ClaimPaymentFunction);
		KTable<String, ArrayList<Customer>> customerGroupedData1 = groupAndAggregateItems(customerKStream1,String.class,Customer.class,CustomerFunction);
		KTable<String, ArrayList<Policy>> policyGrupedData1 = groupAndAggregateItems(policyKStream1,String.class,Policy.class,policyFunction);
		
		/**Join data from aggregated Claim topic and ClaimPayment topic*/

		
		KTable<String, ClaimPaymentClaimJoined> claimpaymentandClainmGrouped1 = claimStrGroupedData1
				.leftJoin(paymentGroupedData1, (claim, payment) -> new ClaimPaymentClaimJoined(payment, claim));
		KTable<String, CustomerPolicyJoined> customerAndPolicyGroupedKTable1 = customerGroupedData1
				.join(policyGrupedData1, (customer, policy) -> new CustomerPolicyJoined(customer, policy));
		KTable<String, ClaimPaymentMap> claimAndPayment2IntGroupedTable1=retriveKeyAggregateData(claimpaymentandClainmGrouped1.toStream());
		KTable<String, CustomerPolicyClaimPayment> finalData1 = customerAndPolicyGroupedKTable1.leftJoin(
				claimAndPayment2IntGroupedTable1, (left, right) -> new CustomerPolicyClaimPayment(left, right));

		
		/**Create complete view for the final data joined from different topics*/
		
		KTable<String, CustomerView> customerView  = createDataView1(finalData1);
		customerView.print();
		customerView.through(Constants.stringSerde, SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(CustomerView.class),
		SerdeUtils.getJSONDeSerializer(CustomerView.class)), Constants.CUSTOMER_VIEW_OUT);
		
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
	private KTable<String, ClaimPaymentMap> retriveKeyAggregateData(KStream<String, ClaimPaymentClaimJoined> stream) throws IllegalAccessException, InstantiationException {
		return stream.groupBy((k, claimPay) -> (claimPay != null)? claimPay.claim.iterator().next().
		CLAIMNUMBER.split("_")[0]: "999", Constants.stringSerde, SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(ClaimPaymentClaimJoined.class),
		SerdeUtils.getJSONDeSerializer(ClaimPaymentClaimJoined.class)))
		.aggregate(ClaimPaymentMap::new, (claimKey, claimPay, claimAndPay2) -> {
				claimAndPay2.add(claimPay);
				return claimAndPay2;
		}, SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(ClaimPaymentList.class),
		SerdeUtils.getJSONDeSerializer(ClaimPaymentMap.class)), Constants.CLAIM_AND_PAYMENT_STORE);
	}
	
	
	/***
	 * get the grouped and aggregated data from Kstream
	 * @param stream
	 * @param keyClass
	 * @param valueClass
	 * @param keyFunc
	 * @param collectionvalueClass
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	
	public <V, K> KTable<K, ArrayList<V>> groupAndAggregateItems(KStream<?, V> stream, Class<K> keyClass, Class<V> valueClass, Function<V, K> keyFunc) throws IllegalAccessException, InstantiationException { 
    
        return stream 
                .selectKey((key, value) -> keyFunc.apply(value)).groupByKey(
               		 SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(keyClass),SerdeUtils.getJSONDeSerializer(keyClass)),
                SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(valueClass),SerdeUtils.getJSONDeSerializer(valueClass)))
                
                .aggregate( 
                        ArrayList::new, 
                        (key, value, aggregate) -> { 
                            aggregate.add(value); 
                            return aggregate; 
                        }, 
                        new ArrayListSerde<>(SerdeUtils.gerJsonSerde(SerdeUtils.getJSONSerializer(valueClass),SerdeUtils.getJSONDeSerializer(valueClass)))
                ); 
        
    } 

	
	/**
	 * Get and properly arrange the Customer data 
	 * @param KTable finalData
	 * @return KTable 
	 */

	private KTable<String, CustomerView> createDataView1(KTable<String, CustomerPolicyClaimPayment> finalData) {
		
		return finalData.<CustomerView>mapValues((all) -> {
			CustomerView view = new CustomerView();
			view.cutomerKey = 	all.customerAndPolicy.customer.iterator().next().CUSTOMER.
			replaceFirst("cust", "");
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


