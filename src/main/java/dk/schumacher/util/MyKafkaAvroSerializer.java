package dk.schumacher.util;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This class can be used to read all schemas from a avro service.
 * @author Gøran Schumacher (GS) / Schumacher Consulting Aps
 * @version $Revision$ 19/06/2018
 */
public class MyKafkaAvroSerializer extends KafkaAvroSerializer {

    public MyKafkaAvroSerializer(Map<String, String> config, boolean isKey) {
        super.configure(config, isKey);
    }

    public Map<String, Schema> getPrimitiveSchemas2() {
        return KafkaAvroSerializer.getPrimitiveSchemas();
    }


    public static void main(String[] args) {
        // Fetches all schemas from an avro service.
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        Map<String, String> config2 = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        MyKafkaAvroSerializer kser = new MyKafkaAvroSerializer(config2, true);
        try {
            Schema kSchema = kser.getBySubjectAndId("Test2",1);

            for (int i = 1; i < 7; i++) {

                System.out.println("Fetched Schema" + i + ": " + kser.getById(i));
            }
            System.out.println("Fetched Schema Name: " + kser.getPrimitiveSchemas2());
            System.out.println("Fetched Schema: " + kSchema);
        } catch (RestClientException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
