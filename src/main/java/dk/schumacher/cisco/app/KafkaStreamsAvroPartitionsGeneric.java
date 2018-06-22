package dk.schumacher.cisco.app;

import dk.schumacher.util.AvroRecordBuilder;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static dk.schumacher.cisco.model.ConstantsGeneric.*;

/*
Demos how to make a join with partitioned topics.

First make the keys the same by Uisng seelectKey, this creates a local kstream.
Then make it into a local kstream and then create a partitioned kstream by using the through() command.

Hopefully this can be simplified!!!

Setup:
1) Create topics with the below script
2) Run KafkaAvroProducer
3) Run this code
 */
public class KafkaStreamsAvroPartitionsGeneric {

    private static final String APP_ID = "DemoApplications106";

    public static void main(String[] args) throws IOException, InterruptedException {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        StreamsConfig config = new StreamsConfig(avroProperties(bootstrapServers, schemaRegistryUrl));
        final Serde<Integer> integerSerde = Serdes.Integer();
        final SpecificAvroSerde avroSerde = new SpecificAvroSerde();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),true);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<Integer, GenericRecord> callDetailStream = kStreamBuilder.stream(integerSerde, valueGenericAvroSerde, TERM_CALL_DETAIL.topicName);

        GlobalKTable<Integer, GenericRecord> callTypeKTable = kStreamBuilder.globalTable(integerSerde, valueGenericAvroSerde, CALL_TYPE.topicName);

        KStream<Integer, GenericRecord> callDetailAndTypeType = callDetailStream.leftJoin(callTypeKTable,
                (key, val) -> (Integer)val.get("callTypeId"),
                (callDetailValue, callTypeValue) -> decodeWholeAvroMessageAvro(callDetailValue, callTypeValue));
        callDetailAndTypeType.print();
        callDetailAndTypeType.to(CISCO_WHOLE.topicName);

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    static private AvroRecordBuilder.Wrapper callDetailWrapper = null;
    static private AvroRecordBuilder.Wrapper callTypeWrapper = null;
    static private AvroRecordBuilder.Wrapper ciscoWholeWrapper = null;
    public static GenericRecord decodeWholeAvroMessageAvro(GenericRecord callDetail, GenericRecord callType)  {
        if(ciscoWholeWrapper == null) {
            callDetailWrapper = AvroRecordBuilder.Wrapper.createFromAvroSchema(callDetail.getSchema());
            callTypeWrapper = AvroRecordBuilder.Wrapper.createFromAvroSchema(callType.getSchema());
            ciscoWholeWrapper = callDetailWrapper.mergeSchema(callTypeWrapper);
        }
        GenericRecord ciscoWhole=ciscoWholeWrapper.copyFields(callDetail, callType);
        System.out.println("ciscoWholeWrapper: " + ciscoWholeWrapper);
        return ciscoWhole;
    }

    public static Properties avroProperties(String bootstrapServers, String schemaRegistryUrl) {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // See: https://www.confluent.io/blog/enabling-exactly-kafka-streams/
        settings.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return settings;
    }

}
