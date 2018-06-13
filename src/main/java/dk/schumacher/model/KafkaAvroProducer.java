package dk.schumacher.model;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.ServerConfig;

import java.io.IOException;
import java.util.Properties;

public class KafkaAvroProducer {

    private final static Schema POLICY_SCHEMA = Schema.parse(dk.schumacher.avro.Constants.POLICY_SCHEMA);
    private final static Schema CUSTOMER_SCHEMA = Schema.parse(dk.schumacher.avro.Constants.CUSTOMER_SCHEMA);

    public static void main(String[] args) throws IOException, InterruptedException {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();

        Properties producerProperties = producerProperties(bootstrapServers, schemaRegistryUrl);
        KafkaProducer<Integer, GenericRecord> producer = new KafkaProducer<Integer, GenericRecord>(producerProperties);

        for (int i = 1; i < 20; i++) {
            GenericRecord customer = new GenericData.Record(CUSTOMER_SCHEMA);
            customer.put("customername", "CustomerName" + i);
            customer.put("customerid", i);
            customer.put("customeraddress", "CustomerAddress" + i);
            customer.put("customertime", System.currentTimeMillis());
            System.out.println(customer);
            producer.send(new ProducerRecord<Integer, GenericRecord>("CustomerDemoAvro", new Integer(i), customer));
        }

        for (int i = 1; i < 20; i++) {
            GenericRecord policy = new GenericData.Record(POLICY_SCHEMA);
            policy.put("customerid", i);
            policy.put("policynumber", "123456" + i);
            policy.put("policytime", System.currentTimeMillis());
            System.out.println(policy);
            producer.send(new ProducerRecord<Integer, GenericRecord>("PolicyDemoAvro", new Integer(i), policy));

        }

        Thread.sleep(1000);
        producer.close();
    }

    // COPIED FROM WikipediaFeedAvroExampleDriver
    private static Properties producerProperties(String bootstrapServers, String schemaRegistryUrl) {
        final Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        prop.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return prop;
    }
}
