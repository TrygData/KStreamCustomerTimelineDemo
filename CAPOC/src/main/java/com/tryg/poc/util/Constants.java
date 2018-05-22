
/** 
 *  @ Constants.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.util;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/** 
 * Description
* @author Joseph James 
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 

public class Constants {
	/*kafka Topics */
	
	//public static final String customerTopic = "STATPEJ.POC_CUSTOMER_DECODED";
	public static final String customerTopic = "demo_customer";
	public static final String PolicyTopic = "STATPEJ.POC_POLICY_DECODED";
	public static final String claimTopic = "STATPEJ.POC_CLAIM_DECODED";
	public static final String claimPaymentTopic = "STATPEJ.POC_CLAIMPAYMENT_DECODED";
	public static final String POLICY_STORE = "PolicyStore";
	public static final String CUSTOMER_STORE = "CustomerStore";
	public static final String CLAIM_STORE = "ClaimStrStore";
	public static final String PAYMENT_STORE = "PaymentStore";
	public static final String CLAIM_AND_PAYMENT_STORE = "claimAndPayment2Store";
	public static final String CUSTOMER_VIEW_OUT = "CA_DEMO_OUTPUT1";
	
	/*serde for serialization*/
	
	public static final Serde<String> stringSerde = Serdes.String();
	public static final Serde<Integer> integerSerde = Serdes.Integer();
	
	/*kafka properties*/
	
	public static  String KafkaServers = "10.84.0.21";
	public static  String KafkaBootStrapPort = "9092";
	public static  String AUTO_OFFSET_RESET_CONFIG="earliest";
}
