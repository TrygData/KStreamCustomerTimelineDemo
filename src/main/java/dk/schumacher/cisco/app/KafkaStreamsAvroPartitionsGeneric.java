package dk.schumacher.cisco.app;

import dk.schumacher.util.AvroRecordBuilder;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

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

        final String CUSTOMER_STORE = "customer-store";

        StreamsConfig config = new StreamsConfig(avroProperties(bootstrapServers, schemaRegistryUrl));
        final Serde<Integer> integerSerde = Serdes.Integer();
        final SpecificAvroSerde avroSerde = new SpecificAvroSerde();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),true);

        StreamsBuilder kStreamBuilder = new StreamsBuilder();

        KStream<Integer, GenericRecord> callDetailStream = kStreamBuilder.stream(callDetailWrapper.topicName, Consumed.with(integerSerde, valueGenericAvroSerde));

        GlobalKTable<Integer, GenericRecord> callTypeKTable = kStreamBuilder.globalTable(callTypeWrapper.topicName, Consumed.with(integerSerde, valueGenericAvroSerde));

        KStream<Integer, GenericRecord> agentTeamMemberStream = kStreamBuilder.stream(agentTeamMemberWrapper.topicName, Consumed.with(integerSerde, valueGenericAvroSerde));

        // Since agentTeamID is PK in AgentTeamMember and we need to join through skillTargetID, we need to select skillTargetID as key.
        agentTeamMemberStream.selectKey((key, value) -> (Integer)value.get("skillTargetID")).to(integerSerde, valueGenericAvroSerde,agentTeamMemberWrapper.topicName+"-2");

        GlobalKTable<Integer, GenericRecord> agentTeamMemberKTable = kStreamBuilder.globalTable(agentTeamMemberWrapper.topicName+"-2", Consumed.with(integerSerde, valueGenericAvroSerde));


        GlobalKTable<Integer, GenericRecord> agentTeamKTable = kStreamBuilder.globalTable(agentTeamWrapper.topicName, Consumed.with(integerSerde, valueGenericAvroSerde));

        // Add CallType - Adds field enterpriseName
        KStream<Integer, GenericRecord> ciscoWhole1 = callDetailStream.leftJoin(callTypeKTable,
                (key, val) -> (Integer)val.get("callTypeId"),
                (callDetailValue, callTypeValue) -> decodeWholeAvroMessageAvro(callDetailValue, callTypeValue));
        ciscoWhole1.print();
        ciscoWhole1.to(ciscoWhole1Wrapper.topicName);

        // Add AgentTeamMember - Adds field agentTeamID
        // This is a left join to a table on a NON PRIMARY KEY - IS THAT POSSIBLE?????
        KStream<Integer, GenericRecord> ciscoWhole2 = ciscoWhole1.leftJoin(agentTeamMemberKTable,
                (key, val) -> (Integer)val.get("agentSkillTargetID"),
                (whole1, agentTeamMember) -> decodeWholeAvroMessageAvro2(whole1, agentTeamMember));
        ciscoWhole2.print();
        ciscoWhole2.to(ciscoWhole2Wrapper.topicName);

        // Add AgentTeam - Adds field EnterpriseName (Agent Team)
        // This is a left join to a table on a NON PRIMARY KEY - IS THAT POSSIBLE?????
        KStream<Integer, GenericRecord> ciscoWhole3 = ciscoWhole2.leftJoin(agentTeamKTable,
                (key, val) -> (Integer)val.get("agentTeamID"),
                (whole2, agentTeam) -> decodeWholeAvroMessageAvro3(whole2, agentTeam));
        ciscoWhole3.print();
        ciscoWhole3.to(ciscoWhole3Wrapper.topicName);

        // Start stream
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder.build(), config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    static private AvroRecordBuilder.Wrapper callDetailWrapper = new AvroRecordBuilder.Wrapper().setTopicName("TermCallDetail-3");
    static private AvroRecordBuilder.Wrapper callTypeWrapper = new AvroRecordBuilder.Wrapper().setTopicName("CallType-3");
    static private AvroRecordBuilder.Wrapper ciscoWhole1Wrapper = new AvroRecordBuilder.Wrapper().setTopicName("Whole1-3");

    public static GenericRecord decodeWholeAvroMessageAvro(GenericRecord callDetail, GenericRecord callType)  {
        if(ciscoWhole1Wrapper.getFields().size() == 0) {
            callDetailWrapper.setSchema(callDetail.getSchema());
            callTypeWrapper.setSchema(callType.getSchema());
            ciscoWhole1Wrapper.setFields(callDetailWrapper.mergeSchema(callTypeWrapper).getFields());
        }
        GenericRecord ciscoWhole1=ciscoWhole1Wrapper.copyFieldsFrom(callDetail, callType);
        System.out.println("ciscoWhole1Wrapper: " + ciscoWhole1Wrapper);
        return ciscoWhole1;
    }

    static private AvroRecordBuilder.Wrapper agentTeamMemberWrapper = new AvroRecordBuilder.Wrapper().setTopicName("AgentTeamMember-3");
    static private AvroRecordBuilder.Wrapper ciscoWhole2Wrapper = new AvroRecordBuilder.Wrapper().setTopicName("Whole2-3");

    public static GenericRecord decodeWholeAvroMessageAvro2(GenericRecord whole1, GenericRecord agentTeamMember)  {
        if(ciscoWhole2Wrapper.getFields().size() == 0) {
            agentTeamMemberWrapper.setSchema(agentTeamMember.getSchema());
            ciscoWhole2Wrapper.setFields(ciscoWhole1Wrapper.mergeSchema(agentTeamMemberWrapper).getFields());
        }
        GenericRecord ciscoWhole2=ciscoWhole2Wrapper.copyFieldsFrom(whole1, agentTeamMember);
        System.out.println("ciscoWhole2Wrapper: " + ciscoWhole2);
        return ciscoWhole2;
    }

    static private AvroRecordBuilder.Wrapper agentTeamWrapper = new AvroRecordBuilder.Wrapper().setTopicName("AgentTeam-4");
    static private AvroRecordBuilder.Wrapper ciscoWhole3Wrapper = new AvroRecordBuilder.Wrapper().setTopicName("Whole3-4");

    public static GenericRecord decodeWholeAvroMessageAvro3(GenericRecord whole2, GenericRecord agentTeam)  {
        if(ciscoWhole3Wrapper.getFields().size() == 0) {
            agentTeamWrapper.setSchema(agentTeam.getSchema());
            ciscoWhole3Wrapper.setFields(ciscoWhole3Wrapper.mergeSchema(agentTeamWrapper).getFields());
        }
        GenericRecord ciscoWhole3=ciscoWhole3Wrapper.copyFieldsFrom(whole2, agentTeam);
        System.out.println("ciscoWhole3Wrapper: " + ciscoWhole3);
        return ciscoWhole3;
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

        settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-global-tables");

        return settings;
    }

}
