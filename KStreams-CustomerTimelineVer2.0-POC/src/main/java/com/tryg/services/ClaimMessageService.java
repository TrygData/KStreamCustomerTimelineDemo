package com.tryg.services;

import static com.tryg.interfaces.ApplicationConstantsInterface.CLAIM_STORE;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;

import com.tryg.model.InputJsonModels.ClaimList;
import com.tryg.model.InputJsonModels.ClaimMessage;
import com.tryg.util.JsonSerdeGeneratorUtil;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ServiceClass for Claim
 */
public class ClaimMessageService {

	// define claimMessageSerde
	final Serde<ClaimMessage> claimMessageSerde = JsonSerdeGeneratorUtil.getSerde(ClaimMessage.class);

	// define claimListSerde
	final Serde<ClaimList> claimListSerde = JsonSerdeGeneratorUtil.getSerde(ClaimList.class);

	public KTable<String, ClaimList> getGroupedClaimData(KStream<Integer, ClaimMessage> claimStream) {

		return claimStream
				.groupBy((k, claim) -> claim.CLAIMNUMBER, Serialized.with(Serdes.String(), claimMessageSerde))
				.aggregate(ClaimList::new, (claimKey, claimMsg, claimLst) -> {
					claimLst.claimRecords.add(claimMsg);
					return (claimLst);
				}, Materialized.as(CLAIM_STORE).with(Serdes.String(),claimListSerde));
	}

	public Serde<ClaimMessage> getClaimMessageSerde() {
		return claimMessageSerde;
	}
	
	
}
