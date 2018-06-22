package com.tryg.services;

import static com.tryg.interfaces.ApplicationConstantsInterface.PAYMENT_STORE;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;

import com.tryg.model.InputJsonModels.PaymentList;
import com.tryg.model.InputJsonModels.PaymentMessage;
import com.tryg.util.JsonSerdeGeneratorUtil;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ServiceClass for Payment
 */
public class PaymentMessageService {

	// define paymentSerde
	final Serde<PaymentMessage> paymentMessageSerde = JsonSerdeGeneratorUtil.getSerde(PaymentMessage.class);

	// define paymentListSerde
	final Serde<PaymentList> paymentListSerde = JsonSerdeGeneratorUtil.getSerde(PaymentList.class);

	public KTable<String, PaymentList> getGroupedPaymentData(KStream<Integer, PaymentMessage> paymentStream) {

		return paymentStream.groupBy((k, payment) -> payment.CLAIMNUMBER, Serialized.with(Serdes.String(), paymentMessageSerde))
				.aggregate(PaymentList::new, (payKey, payMsg, payLst) -> {

					payLst.paymentRecords.add(payMsg);
					return (payLst);
				}, Materialized.as(PAYMENT_STORE).with(Serdes.String(), paymentListSerde));
	}

	public Serde<PaymentMessage> getPaymentMessageSerde() {
		return paymentMessageSerde;
	}
	
	
}
