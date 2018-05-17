package com.tryg.kafkapoc.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListDeserializer<T> implements Deserializer<List<T>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> itemClass;

    public ListDeserializer(Class<T> itemClass) {
        this.itemClass = itemClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        itemClass = (Class<T>) configs.get("itemClass");
    }

    @Override
    public List<T> deserialize(String topic, byte[] data) {
        CollectionType collectionType = objectMapper.getTypeFactory().constructCollectionType(ArrayList.class, itemClass);
        try {
            return data == null ? null : objectMapper.readValue(data, collectionType);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing list of " + itemClass.getSimpleName() + " from JSON", e);
        }
    }

    @Override
    public void close() {

    }
}
