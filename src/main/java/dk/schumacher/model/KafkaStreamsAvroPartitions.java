package dk.schumacher.model;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

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
public class KafkaStreamsAvroPartitions { // Test join of streams

    /* Scripts
    kafka-topics --zookeeper localhost:2181 --delete --topic CustomerDemoAvro
    kafka-topics --zookeeper localhost:2181 --create --topic CustomerDemoAvro --partitions 10 --replication-factor 1

    kafka-topics --zookeeper localhost:2181 --delete --topic PolicyDemoAvro
    kafka-topics --zookeeper localhost:2181 --create --topic PolicyDemoAvro --partitions 10 --replication-factor 1

    kafka-topics --zookeeper localhost:2181 --delete --topic Policy2
    kafka-topics --zookeeper localhost:2181 --create --topic Policy2 --partitions 10 --replication-factor 1

    kafka-topics --zookeeper localhost:2181 --delete --topic Policy3
    kafka-topics --zookeeper localhost:2181 --create --topic Policy3 --partitions 10 --replication-factor 1

    kafka-topics --zookeeper localhost:2181 --delete --topic JoinDemoAvro
    kafka-topics --zookeeper localhost:2181 --create --topic JoinDemoAvro --partitions 10 --replication-factor 1

     */

    private static final String APP_ID = "DemoApplications106";
    private static final String CUSTOMER_TOPIC = "CustomerDemoAvro";
    private static final String POLICY_TOPIC = "PolicyDemoAvro";

    @SuppressWarnings("deprecation")
    private final static Schema CUSTOMER_SCHEMA = Schema.parse(dk.schumacher.avro.Constants.CUSTOMER_SCHEMA);
    @SuppressWarnings("deprecation")
    private final static Schema POLICY_SCHEMA = Schema.parse(dk.schumacher.avro.Constants.POLICY_SCHEMA);
    private final static Schema WHOLE = Schema.parse(dk.schumacher.avro.Constants.WHOLE);

    public static void main(String[] args) throws IOException, InterruptedException {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        StreamsConfig config = new StreamsConfig(avroProperties(bootstrapServers, schemaRegistryUrl));
        final Serde<Integer> integerSerde = Serdes.Integer();
        final SpecificAvroSerde avroSerde = new SpecificAvroSerde();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),true);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<Integer, GenericRecord> customerKstream = kStreamBuilder.stream(integerSerde, valueGenericAvroSerde, CUSTOMER_TOPIC);
        //customerKtable.print();
        KStream<Integer, GenericRecord> policyKStream = kStreamBuilder.stream(integerSerde, valueGenericAvroSerde, POLICY_TOPIC);

        KStream<Integer, GenericRecord> newPolicy = policyKStream.selectKey( (key, value) -> (Integer)value.get("customerid"));  // TEST change key and output as new stream
        newPolicy.to("Policy2");

        // Policy2 is a 'local' topic (can be seen that only partition 1 is used)
        // I think, since we use 10 partitions and NUM_STREAM_THREADS_CONFIG=1 => the code is run 10 times and we only see the lqst run when we look in Policy2 topic???
        // To make it seen in all partitions, you have to use 'through'
        KTable<Integer, GenericRecord> policy2Ktable = kStreamBuilder.table(integerSerde, valueGenericAvroSerde, "Policy2").through("Policy3");
        policy2Ktable.print();

        KStream<Integer, GenericRecord> data2 = customerKstream.join(policy2Ktable, new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>(){
            @Override
            public GenericRecord apply(GenericRecord policyValue, GenericRecord customerValue) {
                try {
                    System.out.println("CustomerValue: " + customerValue);;
                    System.out.println("PolicyValue: " + policyValue);
                    return decodeWholeAvroMessageAvro(customerValue, policyValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        data2.print(integerSerde, avroSerde);
        data2.to(integerSerde, valueGenericAvroSerde, "JoinDemoAvro");

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

    }

    public static GenericRecord decodeWholeAvroMessageAvro(GenericRecord customer, GenericRecord policy) throws IOException {

        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(WHOLE);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord newRecord = new GenericData.Record(WHOLE);

        List<Field> fields = newRecord.getSchema().getFields();
        fields.get(0).schema();

        newRecord.put("CustomerList", customer.toString());
        newRecord.put("policyList", policy.toString());
        System.out.println("Data1: " + customer);
        System.out.println("Data2: " + policy);
        System.out.println("Output: " + newRecord);
        return newRecord;
    }

    private static Properties avroProperties(String bootstrapServers, String schemaRegistryUrl) {
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
