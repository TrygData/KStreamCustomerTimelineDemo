package dk.schumacher.poc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static dk.schumacher.model.Messages.JsonToString;
import static dk.schumacher.model.Messages.createSerde;

/**
 * Copy of example.
 * Create a Topic with 10 partitions manually.
 * Run more than one instance of Stream job and see how groupBy, reduce and aggregate are only performed on the leader node.
 * Start producer (Simple2StreamsSetup) AFTER steam job is started.
 */
public class Simple2Streams {
    static public class PartitionedTopic1 extends JsonToString {
        public Integer PARTITION;
        public Integer SUBGROUP;
        public Integer NUMBER;

        public PartitionedTopic1() {}

        public PartitionedTopic1(Integer PARTITION, Integer SUBGROUP, Integer NUMBER) {
            this.PARTITION=PARTITION;
            this.SUBGROUP=SUBGROUP;
            this.NUMBER=NUMBER;
        }
    }
    static public class PartitionedTopic1List extends ArrayList<PartitionedTopic1> {};

    static public class PartitionedTopic2 extends JsonToString {
        public Integer PARTITION;
        public Integer SUBGROUP;
        public String CHAR;

        public PartitionedTopic2() {}

        public PartitionedTopic2(Integer PARTITION, Integer SUBGROUP, String CHAR) {
            this.PARTITION=PARTITION;
            this.SUBGROUP=SUBGROUP;
            this.CHAR=CHAR;
        }
    }
    static public class PartitionedTopic2List extends ArrayList<PartitionedTopic2> {};

    static public class Topic1And2Joined  extends JsonToString {
        public PartitionedTopic1 topic1;
        public PartitionedTopic2 topic2;

        public Topic1And2Joined(PartitionedTopic1 topic1, PartitionedTopic2 topic2) {
            this.topic1=topic1;
            this.topic2=topic2;
        }
    }

    @JsonIgnoreProperties({ "add" })
    static public class Aggregator extends JsonToString{
        public Integer sum = 0;
        public void add(Integer value) {
            sum = sum + value;
        }
    }

    public static Map<String, Object> serdeProps = new HashMap<String, Object>();
    public static Serde<PartitionedTopic1> partitionedTopic1Serde = createSerde(PartitionedTopic1.class, serdeProps);
    public static Serde<PartitionedTopic1List> partitionedTopic1ListSerde = createSerde(PartitionedTopic1List.class, serdeProps);
    public static Serde<PartitionedTopic2> partitionedTopic2Serde = createSerde(PartitionedTopic2.class, serdeProps);
    public static Serde<PartitionedTopic2List> partitionedTopic2ListSerde = createSerde(PartitionedTopic2List.class, serdeProps);
    public static Serde<Aggregator> aggregatorSerde = createSerde(Aggregator.class, serdeProps);

    private static final String CUSTOMER_TOPIC = "TOPIC1";
    private static final String POLICY_TOPIC = "TOPIC2";

    private static final String CUSTOMER_STORE = "STORE1";
    private static final String POLICY_STORE = "STORE2";

    private static final String CUSTOMER_VIEW_OUT = "CA_DEMO_OUTPUT1";

    public static void main(String[] args) {

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";

        Properties props = streamProperties(bootstrapServers, schemaRegistryUrl);
        System.out.println("Properties: " + props);
        StreamsConfig config = new StreamsConfig(props);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> integerSerde = Serdes.Integer();

        KStreamBuilder kStreamBuilder = new KStreamBuilder();


        /****************************************************************************************************
         * KSTREAMS DEFINITIONS
         ****************************************************************************************************/

        KStream<String, PartitionedTopic1> topic1Stream = kStreamBuilder.stream(stringSerde, partitionedTopic1Serde,
                "PartitionedTopic1");
        KStream<String, PartitionedTopic2> topic2Stream = kStreamBuilder.stream(stringSerde, partitionedTopic2Serde,
                "PartitionedTopic2");
        //topic1Stream.print();
        KGroupedStream<Integer, PartitionedTopic1> groupStream = topic1Stream.groupBy((key, value) -> value.SUBGROUP, integerSerde, partitionedTopic1Serde);
        //groupStream.count().print();
        groupStream.reduce((first, second) -> new PartitionedTopic1(first.PARTITION, first.SUBGROUP, first.NUMBER+second.NUMBER)).print();
        groupStream.aggregate(PartitionedTopic1List::new, (ckey, custMessage, partitionedTopic1List) -> {
            partitionedTopic1List.add(custMessage);
            return partitionedTopic1List;
        }, partitionedTopic1ListSerde, CUSTOMER_STORE);

        groupStream.aggregate(Aggregator::new, (ckey, custMessage, aggregator) -> {
            aggregator.add(custMessage.NUMBER);
            return aggregator;
        }, aggregatorSerde, CUSTOMER_STORE+"RESULT");


//        KTable<String, PartitionedTopic2> topic2Table = kStreamBuilder.table("PartitionedTopic2", "Topic2Table");
//        KStream<String, Topic1And2Joined> joined = topic1Stream.join(topic2Table, (k, v) -> new Topic1And2Joined(k, v) );
//        joined.to("Out");
//        kStreamBuilder.table("Out", "Out");


//        KStream<String, String> policyStream = kStreamBuilder.stream(stringSerde, stringSerde,
//                "PartitionedTopic2");
//        topic1Stream.print();

        /****************************************************************************************************
         * KSTREAMS START STREAMS
         ****************************************************************************************************/

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    private static Properties streamProperties(String bootstrapServers, String schemaRegistryUrl) {

        //Copied from demo
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "AppID");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/tempStore");
        settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 7500 * 1024 * 1024L);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return settings;
    }
}
