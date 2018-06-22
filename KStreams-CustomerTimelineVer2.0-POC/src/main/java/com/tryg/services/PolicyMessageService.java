package com.tryg.services;

import static com.tryg.interfaces.ApplicationConstantsInterface.POLICY_STORE;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;

import com.tryg.model.InputJsonModels.PolicyList;
import com.tryg.model.InputJsonModels.PolicyMessage;
import com.tryg.util.JsonSerdeGeneratorUtil;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * ServiceClass for Policy
 */
public class PolicyMessageService {

	// define policySerde
	final Serde<PolicyMessage> policyMessageSerde = JsonSerdeGeneratorUtil.getSerde(PolicyMessage.class);

	// define policyListSerde
	final Serde<PolicyList> policyListSerde = JsonSerdeGeneratorUtil.getSerde(PolicyList.class);

	public KTable<Integer, PolicyList> getGroupedPolicyData(KStream<Integer, PolicyMessage> policyStream) {

		return policyStream.groupBy((k, policy) -> policy.POLICY, Serialized.with(Serdes.Integer(), policyMessageSerde))
				.aggregate(PolicyList::new, (policyKey, policyMsg, policyLst) -> {
					policyLst.policyRecords.add(policyMsg);
					return (policyLst);
				}, Materialized.as(POLICY_STORE).with(Serdes.Integer(), policyListSerde));
	}

	public Serde<PolicyMessage> getPolicyMessageSerde() {
		return policyMessageSerde;
	}
	
	
}
