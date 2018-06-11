package dk.schumacher.model;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class KafkaAvroConsumer {
    @SuppressWarnings("deprecation")
    private final static Schema CUSTOMER_SCHEMA = Schema.parse(dk.schumacher.avro.Constants.CUSTOMER_SCHEMA);

    public static void main(String[] args) throws IOException {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        Properties consumerProperties = consumerProperties(bootstrapServers, schemaRegistryUrl, "avro-consumer-group-id");
        System.out.println("Properties: " + consumerProperties);
        KafkaConsumer<Integer, GenericRecord> consumer = new KafkaConsumer<Integer, GenericRecord>(consumerProperties);
        consumer.subscribe(Arrays.asList("CustomerDemoAvro"));
        int i = 0;
        while (true) {
            ConsumerRecords<Integer, GenericRecord> records = consumer.poll(1000);
            for (ConsumerRecord<Integer, GenericRecord> record : records) {
                System.out.println(record);
            }
        }
    }

    public static GenericRecord decodeAvroMessage(byte[] message) throws IOException {
        GenericDatumReader<GenericRecord> dataReader = new GenericDatumReader<GenericRecord>(CUSTOMER_SCHEMA);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        GenericRecord recordFromEvent = dataReader.read(null, decoder);
        return recordFromEvent;
    }

    private static Properties consumerProperties(String bootstrapServers, String schemaRegistryUrl, String groupId) {
        final Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.ByteArray());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "My_GROUP_ID"); // GroupID should contain version number of the consumer. So when there is a new version, the consumer will read from start
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        prop.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return prop;
    }
}
