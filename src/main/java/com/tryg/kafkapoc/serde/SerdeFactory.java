package com.tryg.kafkapoc.serde;

import com.tryg.kafkapoc.model.GenericCollections;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class SerdeFactory {

    public <T> Serde<T> getSerde(Class<T> targetClass) {
        try {
            return Serdes.serdeFrom(targetClass);
        } catch (IllegalArgumentException e) { //Proper input validation should be made instead of catching the exception
            return getJsonSerde(targetClass);
        }
    }

    public <T> Serde<Collection<T>> getCollectionSerde(Class<T> itemClass) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new CollectionDeserializer<>(itemClass));
    }

    public <V1, V2> Serde<GenericCollections<V1, V2>> getGenericListsViewSerde(Class<V1> itemClass1, Class<V2> itemClass2) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new GenericCollectionsDeserializer<>(itemClass1, itemClass2));
    }

    private <T> Serde<T> getJsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(targetClass));
    }
}