package dk.schumacher.cisco.app;

import dk.schumacher.util.AvroRecordBuilder;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static dk.schumacher.cisco.model.ConstantsGeneric.AGENT_TEAM_MEMBER;
import static dk.schumacher.cisco.model.ConstantsGeneric.CALL_TYPE;
import static dk.schumacher.cisco.model.ConstantsGeneric.TERM_CALL_DETAIL;

public class KafkaAvroProducerGeneric {

    /* Scripts
    kafka-topics --zookeeper localhost:2181 --delete --topic TermCallDetail
    kafka-topics --zookeeper localhost:2181 --create --topic TermCallDetail --partitions 10 --replication-factor 1

    kafka-topics --zookeeper localhost:2181 --delete --topic CallType
    kafka-topics --zookeeper localhost:2181 --create --topic CallType --partitions 1 --replication-factor 1

    kafka-topics --zookeeper localhost:2181 --delete --topic AgentTeamMember
    kafka-topics --zookeeper localhost:2181 --create --topic AgentTeamMember --partitions 1 --replication-factor 1

     */

    public static void main(String[] args) throws IOException, InterruptedException {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();

        Properties producerProperties = producerProperties(bootstrapServers, schemaRegistryUrl);
        KafkaProducer<Integer, GenericRecord> producer = new KafkaProducer<Integer, GenericRecord>(producerProperties);

        for (int i = 1; i < 20; i++) {

            // TERM_CALL_DETAIL
            GenericRecord callDetail = TERM_CALL_DETAIL.getGenericRecord();
            callDetail.put("agentSkillTargetID", generate_agentSkillTargetID(i));
            callDetail.put("callTypeId", generate_agentSkillTargetID(i));
            callDetail.put("aNI", randomNumber(8));
            callDetail.put("digitsDialed", generate_skillTargetID(i));
            System.out.println(callDetail);
            System.out.println("TERM_CALL_DETAIL_SCHEMA1: " + callDetail.getSchema().getFields().get(1).schema());
            AvroRecordBuilder.Wrapper w = AvroRecordBuilder.Wrapper.createFromAvroSchema(callDetail.getSchema());
            System.out.println("TERM_CALL_DETAIL_SCHEMA2: " + w);
            producer.send(new ProducerRecord<Integer, GenericRecord>(TERM_CALL_DETAIL.topicName, new Integer(i), callDetail));

            // CALL_TYPE
            GenericRecord callType = CALL_TYPE.getGenericRecord();
            callType.put("callTypeId", generate_agentSkillTargetID(i));
            callType.put("enterpriseName", "Call type Description: " + i);
            System.out.println(callType);
            producer.send(new ProducerRecord<Integer, GenericRecord>(CALL_TYPE.topicName, new Integer(i), callType));

            // AGENT_TEAM_MEMBER
            GenericRecord agentTeamMember = AGENT_TEAM_MEMBER.getGenericRecord();
            agentTeamMember.put("agentTeamID", generate_agentSkillTargetID(i));
            agentTeamMember.put("skillTargetID", generate_agentSkillTargetID(i)+10);
            System.out.println(callType);
            producer.send(new ProducerRecord<Integer, GenericRecord>(AGENT_TEAM_MEMBER.topicName, new Integer(i), agentTeamMember));
        }

        Thread.sleep(1000);
        producer.close();
    }

    // COPIED FROM WikipediaFeedAvroExampleDriver
    private static Properties producerProperties(String bootstrapServers, String schemaRegistryUrl) {
        final Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        prop.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return prop;
    }

    static private int generate_agentSkillTargetID(int seed) {
        return seed % 20;
    }

    static private String generate_skillTargetID(int seed) {
        return String.valueOf(seed % 20);
    }

    static private String generate_digitsDialed(int seed) {
        return String.valueOf(seed % 20);
    }

    static public String randomNumber(int length) {
        StringBuffer buff = new StringBuffer();
        Random rand = new Random();
        for (int i = 0; i < length; i++) {
            buff.append(rand.nextInt(9)+1);
        }
        return buff.toString();
    }
}
