package com.tryg.kafkapoc.config;

import com.tryg.kafkapoc.serde.JsonSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaPropertiesFactory {
    public static Properties getProducerProperties(Class keyClass, Class valueClass) {
        Properties props = getDefaultProps();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializerClass(keyClass));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializerClass(valueClass));

        return props;
    }

    public static Properties getFullProperties() {
        Properties props = getDefaultProps();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "POC-1");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }

    private static Properties getDefaultProps() {
        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return props;
    }

    private static Class<? extends Serializer> getSerializerClass(Class classToSerialize) {
        switch (classToSerialize.getSimpleName()) {
            case "Double":
                return DoubleSerializer.class;
            case "Integer":
                return IntegerSerializer.class;
            case "String":
                return StringSerializer.class;
            default:
                return JsonSerializer.class;
        }
    }
}
