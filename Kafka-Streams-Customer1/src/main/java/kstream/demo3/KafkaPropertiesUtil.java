package kstream.demo3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaPropertiesUtil {

    private static final String APP_ID = String.valueOf(System.currentTimeMillis());

    private static final String LEVEL = "DEBUG";


    public static Properties getProperties() {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "wn0-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn1-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn2-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092,wn3-kaf001.si34agdvrydetneynufspu4j5a.fx.internal.cloudapp.net:9092");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/tempStore");
        settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 7500 * 1024 * 1024L);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // settings.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
        settings.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
        // settings.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
        settings.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
        // settings.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,"metric.reporters");
        settings.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, LEVEL);
        return settings;
    }
}
