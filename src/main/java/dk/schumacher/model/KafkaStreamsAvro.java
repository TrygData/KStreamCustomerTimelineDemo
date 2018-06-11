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


public class KafkaStreamsAvro {

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
        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
        final SpecificAvroSerde avroSerde = new SpecificAvroSerde();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),true);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KTable<String, GenericRecord> customerKtable = kStreamBuilder.table(stringSerde, valueGenericAvroSerde, CUSTOMER_TOPIC);
        customerKtable.print();
        KStream<String, GenericRecord> policyKStream = kStreamBuilder.stream(stringSerde, valueGenericAvroSerde, POLICY_TOPIC);

        KStream<String, GenericRecord> neww = policyKStream.selectKey( (key, value) -> String.valueOf(new Random().nextInt(100)));  // TEST change key and output as new stream
        neww.to("TEST");

        KStream<String, GenericRecord> data = policyKStream.join(customerKtable, new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>(){
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

        data.print(Serdes.String(), avroSerde);
        data.to(stringSerde, valueGenericAvroSerde, "FINAL_DEMO_OUTPUT105");

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
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
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
