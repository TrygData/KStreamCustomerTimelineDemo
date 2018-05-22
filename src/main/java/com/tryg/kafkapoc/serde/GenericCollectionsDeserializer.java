package com.tryg.kafkapoc.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.tryg.kafkapoc.model.GenericCollections;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GenericCollectionsDeserializer<V1, V2> implements Deserializer<GenericCollections<V1, V2>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Class<V1> itemClass1;
    private Class<V2> itemClass2;

    public GenericCollectionsDeserializer(Class<V1> itemClass1, Class<V2> itemClass2) {
        this.itemClass1 = itemClass1;
        this.itemClass2 = itemClass2;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public GenericCollections<V1, V2> deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        try {
            JsonNode rootNode = objectMapper.readTree(data);

            JsonNode listNode1 = rootNode.get("list1");
            JsonNode listNode2 = rootNode.get("list2");

            CollectionType collectionType1 = objectMapper.getTypeFactory().constructCollectionType(List.class, itemClass1);
            CollectionType collectionType2 = objectMapper.getTypeFactory().constructCollectionType(List.class, itemClass2);

            List<V1> list1 = objectMapper.readValue(objectMapper.treeAsTokens(listNode1), collectionType1);
            List<V2> list2 = objectMapper.readValue(objectMapper.treeAsTokens(listNode2), collectionType2);

            return new GenericCollections<>(list1, list2);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing GenericCollections of " + itemClass1.getSimpleName() + " and " + itemClass2.getSimpleName() + " from JSON", e);
        }
    }

    @Override
    public void close() {

    }
}
