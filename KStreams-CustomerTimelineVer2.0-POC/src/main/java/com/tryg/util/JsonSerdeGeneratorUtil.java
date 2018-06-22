package com.tryg.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import com.tryg.serde.JsonPOJODeserializer;
import com.tryg.serde.JsonPOJOSerializer;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * UtilClass for JsonSerdeGenerator
 */
public final class JsonSerdeGeneratorUtil {

	public static <T> Serde<T> getSerde(Class<T> t) {
		// define customerListSerde
		Map<String, Class<T>> serdeProps = new HashMap<String, Class<T>>();
		final Serializer<T> serializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", t);
		serializer.configure(serdeProps, false);

		final Deserializer<T> deserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", t);
		deserializer.configure(serdeProps, false);
		final Serde<T> serde = Serdes.serdeFrom(serializer,
				deserializer);
		return serde;
	}

}
