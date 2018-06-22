package com.tryg.services;

import static com.tryg.interfaces.ApplicationConstantsInterface.CLAIM_AND_PAYMENT_STORE;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;

import com.tryg.model.IntermediateJsonModel.ClaimAndPayment;
import com.tryg.model.OutputJsonModel.ClaimAndPayment2;
import com.tryg.util.JsonSerdeGeneratorUtil;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ServiceClass for ClaimAndPayment
 */
public class ClaimAndPayment2Service {

	// define claimAndPaymentSerde
	final Serde<ClaimAndPayment> claimAndPaymentSerde = JsonSerdeGeneratorUtil.getSerde(ClaimAndPayment.class);

	// define claimAndPayment2Serde
	final Serde<ClaimAndPayment2> claimAndPayment2Serde = JsonSerdeGeneratorUtil.getSerde(ClaimAndPayment2.class);

	public KTable<Integer, ClaimAndPayment2> getGroupedClaimAndPaymentData(KStream<String, ClaimAndPayment> claimAndPaymentKStream) {

		return claimAndPaymentKStream
				.groupBy((k, claimPay) -> (claimPay != null)
						? Integer.parseInt(claimPay.claimList.claimRecords.get(0).CLAIMNUMBER.split("_")[0])
						: 999, Serialized.with(Serdes.Integer(), claimAndPaymentSerde))
				.aggregate(ClaimAndPayment2::new, (claimKey, claimPay, claimAndPay2) -> {
					claimAndPay2.add(claimPay);
					return claimAndPay2;
				}, Materialized.as(CLAIM_AND_PAYMENT_STORE).with(Serdes.Integer(), claimAndPayment2Serde));
	}

}
