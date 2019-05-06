package com.github.felipearomani.kafkastreams.zmart;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class YellingApp {

    private static final Logger LOG = LoggerFactory.getLogger(YellingApp.class);

    public static void main(String[] args) throws InterruptedException {

        // Create stream config
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        StreamsConfig streamsConfig = new StreamsConfig(props);

        // Get the default String Serde
        Serde<String> stringSerde = Serdes.String();

        // Construct the source, processor and sync
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("src-topic", Consumed.with(stringSerde, stringSerde))
                .mapValues((ValueMapper<String, String>) String::toUpperCase)
                .to("out-topic", Produced.with(stringSerde, stringSerde));

        // Create and start stream
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);

        kafkaStreams.start();
        Thread.sleep(35000);
        LOG.info("Shutting down the Yelling APP now");
        kafkaStreams.close();
    }
}
