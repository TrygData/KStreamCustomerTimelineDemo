package com.tryg.interfaces;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * Inerface for Application Constants
 */


public interface ApplicationConstantsInterface {
	public static final String APP_ID = String.valueOf(System.currentTimeMillis());
	public static final String CUSTOMER_TOPIC = "STATPEJ.POC_CUSTOMER_DECODED";
	public static final String POLICY_TOPIC = "STATPEJ.POC_POLICY_DECODED";
	public static final String CLAIM_TOPIC = "STATPEJ.POC_CLAIM_DECODED";
	public static final String PAYMENT_TOPIC = "STATPEJ.POC_CLAIMPAYMENT_DECODED";
	public static final String CUSTOMER_VIEW_OUT = "CA_DEMO_OUTPUT1";
	public static final String CUSTOMER_STORE = "CustomerStore";
	public static final String POLICY_STORE = "PolicyStore";
	public static final String CLAIM_STORE = "ClaimStrStore";
	public static final String PAYMENT_STORE = "PaymentStore";
	public static final String CLAIM_AND_PAYMENT_STORE = "claimAndPayment2Store";
	public static final String LEVEL = "DEBUG";
	public static final String PROPERTY_FILE_PATH = "conf/applicationConfig.properties";

	public static interface PropertyFileKeys {
		public static final String BOOTSTRAP_SERVERS_CONFIG = "BOOTSTRAP_SERVERS_CONFIG";
		public static final String STATE_DIR_CONFIG = "STATE_DIR_CONFIG";
		public static final String STREAMS_NUM_STREAM_THREADS_CONFIG = "STREAMS_NUM_STREAM_THREADS_CONFIG";
		public static final String STREAMS_CACHE_MAX_BYTES_BUFFERING_CONFIG = "STREAMS_CACHE_MAX_BYTES_BUFFERING_CONFIG";
		public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG = "CONSUMER_AUTO_OFFSET_RESET_CONFIG";
		public static final String CONSUMER_METRICS_RECORDING_LEVEL_CONFIG = "CONSUMER_METRICS_RECORDING_LEVEL_CONFIG";
		public static final String STREAMS_METRICS_RECORDING_LEVEL_CONFIG = "STREAMS_METRICS_RECORDING_LEVEL_CONFIG";
		public static final String PRODUCER_METRICS_RECORDING_LEVEL_CONFIG = "PRODUCER_METRICS_RECORDING_LEVEL_CONFIG";
	}

}
