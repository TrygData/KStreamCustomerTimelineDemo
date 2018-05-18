package com.tryg.kafkapoc.config;

import com.tryg.kafkapoc.serde.SerdeFactory;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Component
public class StreamsBuilderUtil {
    private final KafkaPropertiesFactory propsFactory;
    private final SerdeFactory serdeFactory;
    private final StreamsBuilder streamsBuilder;

    public StreamsBuilderUtil(KafkaPropertiesFactory propsFactory, SerdeFactory serdeFactory, StreamsBuilder streamsBuilder) {
        this.propsFactory = propsFactory;
        this.serdeFactory = serdeFactory;
        this.streamsBuilder = streamsBuilder;
    }

    public <K, V> KStream<K, V> readTopic(String topicName, Class<K> keyClass, Class<V> valueClass) {
        return streamsBuilder.stream(topicName, Consumed.with(serdeFactory.getSerde(keyClass), serdeFactory.getSerde(valueClass)));
    }

    public <V, K> KTable<K, List<V>> groupAndAggregateInList(KStream<?, V> stream, Class<K> keyClass, Class<V> valueClass, Function<V, K> keyFunc) {
        return stream
                .groupBy(
                        (key, value) -> keyFunc.apply(value),
                        Serialized.with(serdeFactory.getSerde(keyClass), serdeFactory.getSerde(valueClass))
                )
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        Materialized.with(serdeFactory.getSerde(keyClass), serdeFactory.getListSerde(valueClass))
                );
    }

    public void start() {
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), propsFactory.getFullProperties());
        kafkaStreams.start();
    }
}