package com.tryg.services;

import static com.tryg.interfaces.ApplicationConstantsInterface.CUSTOMER_STORE;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;

import com.tryg.model.InputJsonModels.CustomerList;
import com.tryg.model.InputJsonModels.CustomerMessage;
import com.tryg.util.JsonSerdeGeneratorUtil;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ServiceClass for Customer
 */
public class CustomerMessageService {

	// define CustomerMessageSerde
	final Serde<CustomerMessage> customerMessageSerde = JsonSerdeGeneratorUtil.getSerde(CustomerMessage.class);

	// define customerListSerde
	final Serde<CustomerList> customerListSerde = JsonSerdeGeneratorUtil.getSerde(CustomerList.class);

	public KTable<Integer, CustomerList> getGroupedConsumerData(KStream<String, CustomerMessage> customerStream) {
		return customerStream
				.groupBy((key, value) -> Integer.parseInt(value.POLICY),
						Serialized.with(Serdes.Integer(), customerMessageSerde))
				.aggregate(CustomerList::new, (ckey, custMessage, customerList) -> {
					customerList.customerRecords.add(custMessage);
					return customerList;
				}, Materialized.as(CUSTOMER_STORE).with(Serdes.Integer(), customerListSerde));
	}

	public Serde<CustomerMessage> getCustomerMessageSerde() {
		return customerMessageSerde;
	}
	
	
}
