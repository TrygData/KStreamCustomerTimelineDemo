package com.tryg.kafkapoc.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Serialized;

public class SerdeFactory {
    public static <T> Serde<T> getSerde(Class<T> targetClass) {
        try {
            return Serdes.serdeFrom(targetClass);
        } catch (IllegalArgumentException e) { //Proper input validation should be made instead of catching the exception
            return getJsonSerde(targetClass);
        }
    }

    public static <T> Serde<T> getJsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(targetClass));
    }

    public static <K, V> Serialized<K, V> getSerialized(Class<K> keyClass, Class<V> valueClass) {
        return Serialized.with(getSerde(keyClass), getSerde(valueClass));
    }
}