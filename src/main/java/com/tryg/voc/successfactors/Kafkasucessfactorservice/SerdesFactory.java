package com.tryg.voc.successfactors.Kafkasucessfactorservice;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SerdesFactory<T>  {


    private T t;


    public static final String JSON_POJO_CLASS = "JsonPOJOClass";
    private static Map<String, Object> serdeProps;


    public Serde<T> serdes(Class<? extends Object> class2){
        serdeProps = new HashMap<>();
        serdeProps.put(JSON_POJO_CLASS, class2);
        final Serializer<T> tSerializer = new JsonPOJOSerializer<>();
        tSerializer.configure(serdeProps, false);

        final Deserializer<T> tDeserializer = new JsonPOJODeserializer<>();
        tDeserializer.configure(serdeProps, false);

        final  Serde<T> tSerde = Serdes.serdeFrom(tSerializer, tDeserializer);
        return  tSerde;

    }
}
